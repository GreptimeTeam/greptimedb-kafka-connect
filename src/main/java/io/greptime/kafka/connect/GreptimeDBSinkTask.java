/*
 * Copyright 2026 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.kafka.connect;

import io.greptime.GreptimeDB;
import io.greptime.models.AuthInfo;
import io.greptime.models.Err;
import io.greptime.models.Result;
import io.greptime.models.Table;
import io.greptime.models.WriteOk;
import io.greptime.options.GreptimeOptions;
import io.greptime.rpc.TlsOptions;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes each {@link #put} batch synchronously, so committed offsets only cover written records. A
 * retryable failure throws {@link RetriableException} and the framework redelivers the same batch.
 */
public class GreptimeDBSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(GreptimeDBSinkTask.class);

    // Connect framework setting; the task sees it because task configs are the connector config.
    private static final String ERRORS_TOLERANCE = "errors.tolerance";

    private GreptimeDBSinkConfig config;
    private RecordConverter converter;
    private GreptimeDB client;
    private ErrantRecordReporter reporter;
    private boolean tolerateBadRecords;
    private int remainingRetries;
    // Bad records already handled in the batch being retried, so redeliveries do not report them again.
    private final Set<String> handledBadRecords = new HashSet<>();

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new GreptimeDBSinkConfig(props);
        this.converter = new RecordConverter(this.config);
        this.reporter = errantRecordReporter();
        this.tolerateBadRecords = "all".equalsIgnoreCase(props.get(ERRORS_TOLERANCE));
        this.remainingRetries = this.config.maxRetries();
        this.client = GreptimeDB.create(clientOptions(this.config));
        LOG.info(
                "Started GreptimeDB sink task, endpoints={}, database={}",
                this.config.endpoints(),
                this.config.database());
    }

    private ErrantRecordReporter errantRecordReporter() {
        try {
            return this.context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Connect workers before 2.6 have no errant record reporter.
            return null;
        }
    }

    static GreptimeOptions clientOptions(GreptimeDBSinkConfig config) {
        GreptimeOptions.Builder builder = GreptimeOptions.newBuilder(
                        config.endpoints().toArray(new String[0]), config.database())
                // Retries are driven by this task; see WriteErrors for why the ingester's are not used.
                .writeMaxRetries(0);
        if (!config.username().isEmpty()) {
            builder.authInfo(new AuthInfo(config.username(), config.password().value()));
        }
        if (config.tlsEnabled()) {
            TlsOptions tls = new TlsOptions();
            if (!config.tlsCaCertPath().isEmpty()) {
                tls.setRootCerts(new File(config.tlsCaCertPath()));
            }
            builder.tlsOptions(tls);
        }
        return builder.build();
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        Map<String, TableBatch> batches = new LinkedHashMap<>();
        for (SinkRecord record : records) {
            if (record.value() == null
                    && this.config.nullValueBehavior() == GreptimeDBSinkConfig.NullValueBehavior.IGNORE) {
                continue;
            }
            try {
                if (record.value() == null) {
                    throw new DataException(
                            "Record value is null and " + GreptimeDBSinkConfig.BEHAVIOR_ON_NULL_VALUES + " is fail");
                }
                RecordConverter.Row row = this.converter.convert(record);
                batches.computeIfAbsent(row.table, TableBatch::new).add(row, record);
            } catch (DataException e) {
                handleBadRecord(record, e);
            }
        }

        int batchSize = this.config.batchSize();
        for (TableBatch batch : batches.values()) {
            for (int from = 0; from < batch.size(); from += batchSize) {
                writeRange(batch, from, Math.min(from + batchSize, batch.size()));
            }
        }
        this.remainingRetries = this.config.maxRetries();
        this.handledBadRecords.clear();
    }

    private void handleBadRecord(SinkRecord record, DataException e) {
        String where = String.format(
                "topic=%s, partition=%d, offset=%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        if (!this.tolerateBadRecords) {
            throw new DataException("Bad record (" + where + "): " + e.getMessage(), e);
        }
        if (!this.handledBadRecords.add(where)) {
            return;
        }
        if (this.reporter == null) {
            LOG.warn("Skipping bad record ({}): {}", where, e.getMessage());
            return;
        }
        LOG.warn("Reporting bad record ({}): {}", where, e.getMessage());
        this.reporter.report(record, e);
    }

    /** Writes rows {@code [from, to)} of the batch. */
    private void writeRange(TableBatch batch, int from, int to) {
        Table table = batch.toTable(from, to);
        WriteFailure failure = write(table);
        if (failure == null) {
            return;
        }
        if (failure.kind == WriteErrors.Kind.DATA && this.tolerateBadRecords) {
            if (to - from == 1) {
                handleBadRecord(batch.record(from), new DataException(failure.message, failure.cause));
                return;
            }
            int mid = (from + to) >>> 1;
            writeRange(batch, from, mid);
            writeRange(batch, mid, to);
            return;
        }
        throw toException(failure);
    }

    /** Returns null when the write succeeded. */
    private WriteFailure write(Table table) {
        Result<WriteOk, Err> result;
        try {
            result = this.client.write(table).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while writing to table " + table.tableName(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return new WriteFailure(table, WriteErrors.classify(cause), String.valueOf(cause), cause);
        }
        if (result.isOk()) {
            return null;
        }
        Err err = result.getErr();
        return new WriteFailure(table, WriteErrors.classifyStatusCode(err.getCode()), err.toString(), err.getError());
    }

    private ConnectException toException(WriteFailure failure) {
        if (failure.kind != WriteErrors.Kind.RETRYABLE) {
            return new ConnectException(failure.message + " (not retryable)", failure.cause);
        }
        if (this.remainingRetries <= 0) {
            return new ConnectException(
                    failure.message + " (retries exhausted after " + this.config.maxRetries() + " attempts)",
                    failure.cause);
        }
        this.remainingRetries--;
        LOG.warn(
                "{}; retrying the batch in {} ms, {} retries left",
                failure.message,
                this.config.retryBackoffMs(),
                this.remainingRetries);
        this.context.timeout(this.config.retryBackoffMs());
        return new RetriableException(failure.message, failure.cause);
    }

    private static final class WriteFailure {
        final WriteErrors.Kind kind;
        final String message;
        final Throwable cause;

        WriteFailure(Table table, WriteErrors.Kind kind, String detail, Throwable cause) {
            this.kind = kind;
            this.message = String.format(
                    "Failed to write %d rows to table %s: %s", table.rowCount(), table.tableName(), detail);
            this.cause = cause;
        }
    }

    @Override
    public void stop() {
        if (this.client != null) {
            this.client.shutdownGracefully();
            this.client = null;
        }
    }
}
