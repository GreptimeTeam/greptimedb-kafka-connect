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
import java.util.LinkedHashMap;
import java.util.Map;
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
 * Writes each {@link #put} batch synchronously, so offsets committed by the framework only cover written
 * records (at-least-once). A retryable write failure throws {@link RetriableException}, which makes the
 * framework redeliver the same batch after {@code retry.backoff.ms}. Rows written before the failure are
 * written again; tables with the default (non-append) mode deduplicate them by primary key and time index.
 */
public class GreptimeDBSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(GreptimeDBSinkTask.class);

    private GreptimeDBSinkConfig config;
    private RecordConverter converter;
    private GreptimeDB client;
    private ErrantRecordReporter reporter;
    private int remainingRetries;

    @Override
    public String version() {
        return Version.get();
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new GreptimeDBSinkConfig(props);
        this.converter = new RecordConverter(this.config);
        this.reporter = errantRecordReporter();
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
                batches.computeIfAbsent(row.table, TableBatch::new).add(row);
            } catch (DataException e) {
                reportBadRecord(record, e);
            }
        }

        for (TableBatch batch : batches.values()) {
            for (Table table : batch.toTables(this.config.batchSize())) {
                write(table);
            }
        }
        this.remainingRetries = this.config.maxRetries();
    }

    private void reportBadRecord(SinkRecord record, DataException e) {
        String where = String.format(
                "topic=%s, partition=%d, offset=%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
        if (this.reporter == null) {
            throw new DataException("Failed to convert record (" + where + "): " + e.getMessage(), e);
        }
        LOG.warn("Reporting bad record ({}): {}", where, e.getMessage());
        // Throws when errors.tolerance=none, failing the task.
        this.reporter.report(record, e);
    }

    private void write(Table table) {
        Result<WriteOk, Err> result;
        try {
            result = this.client.write(table).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while writing to table " + table.tableName(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw writeFailure(table, WriteErrors.isRetryable(cause), String.valueOf(cause), cause);
        }
        if (!result.isOk()) {
            Err err = result.getErr();
            throw writeFailure(table, WriteErrors.isRetryableStatusCode(err.getCode()), err.toString(), err.getError());
        }
    }

    private ConnectException writeFailure(Table table, boolean retryable, String detail, Throwable cause) {
        String message =
                String.format("Failed to write %d rows to table %s: %s", table.rowCount(), table.tableName(), detail);
        if (!retryable) {
            return new ConnectException(message + " (not retryable)", cause);
        }
        if (this.remainingRetries <= 0) {
            return new ConnectException(
                    message + " (retries exhausted after " + this.config.maxRetries() + " attempts)", cause);
        }
        this.remainingRetries--;
        LOG.warn(
                "{}; retrying the batch in {} ms, {} retries left",
                message,
                this.config.retryBackoffMs(),
                this.remainingRetries);
        this.context.timeout(this.config.retryBackoffMs());
        return new RetriableException(message, cause);
    }

    @Override
    public void stop() {
        if (this.client != null) {
            this.client.shutdownGracefully();
            this.client = null;
        }
    }
}
