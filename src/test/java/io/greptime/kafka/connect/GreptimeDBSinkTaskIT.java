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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class GreptimeDBSinkTaskIT {

    @Container
    static final GreptimeDBContainer GREPTIMEDB = new GreptimeDBContainer();

    private static final Schema CPU_SCHEMA = SchemaBuilder.struct()
            .field("ts", Schema.INT64_SCHEMA)
            .field("host", Schema.STRING_SCHEMA)
            .field("usage", Schema.FLOAT64_SCHEMA)
            .build();

    private final List<GreptimeDBSinkTask> tasks = new ArrayList<>();

    @AfterEach
    void stopTasks() {
        tasks.forEach(GreptimeDBSinkTask::stop);
    }

    private GreptimeDBSinkTask startTask(StubContext context, String... kv) {
        Map<String, String> props = new HashMap<>();
        props.put(GreptimeDBSinkConfig.ENDPOINTS, GREPTIMEDB.grpcEndpoint());
        for (int i = 0; i < kv.length; i += 2) {
            props.put(kv[i], kv[i + 1]);
        }
        GreptimeDBSinkTask task = new GreptimeDBSinkTask();
        task.initialize(context);
        task.start(props);
        tasks.add(task);
        return task;
    }

    private static SinkRecord record(String topic, Schema schema, Object value, long offset) {
        return new SinkRecord(topic, 0, null, null, schema, value, offset, null, TimestampType.NO_TIMESTAMP_TYPE);
    }

    private static SinkRecord cpu(String topic, long ts, String host, double usage, long offset) {
        Struct v = new Struct(CPU_SCHEMA).put("ts", ts).put("host", host).put("usage", usage);
        return record(topic, CPU_SCHEMA, v, offset);
    }

    @Test
    void writesRowsInBatchesAndReplayIsIdempotent() throws Exception {
        GreptimeDBSinkTask task = startTask(
                new StubContext(null),
                GreptimeDBSinkConfig.TIMESTAMP_FIELD,
                "ts",
                GreptimeDBSinkConfig.TAG_FIELDS,
                "host",
                GreptimeDBSinkConfig.BATCH_SIZE,
                "2");

        List<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(cpu("cpu", 1_700_000_000_000L + i, "h" + (i % 2), i + 0.5, i));
        }
        task.put(records);
        // Redelivery after a retry or a task restart writes the same rows again.
        task.put(records);

        assertEquals(
                Arrays.asList(
                        Arrays.asList("h0", "0.5"),
                        Arrays.asList("h1", "1.5"),
                        Arrays.asList("h0", "2.5"),
                        Arrays.asList("h1", "3.5"),
                        Arrays.asList("h0", "4.5")),
                GREPTIMEDB.query("SELECT host, usage FROM cpu ORDER BY ts"));
        assertEquals(
                Arrays.asList(
                        Arrays.asList("ts", "TIMESTAMP"),
                        Arrays.asList("host", "TAG"),
                        Arrays.asList("usage", "FIELD")),
                GREPTIMEDB.query("SELECT column_name, semantic_type FROM information_schema.columns "
                        + "WHERE table_name = 'cpu' ORDER BY ordinal_position"));
    }

    @Test
    void schemaMismatchWithExistingTableIsNotRetried() {
        GreptimeDBSinkTask task = startTask(new StubContext(null), GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts");
        task.put(Collections.singletonList(record("mismatch", null, map("ts", 1L, "v", 1L), 0)));

        ConnectException e = assertThrows(
                ConnectException.class,
                () -> task.put(Collections.singletonList(record("mismatch", null, map("ts", 2L, "v", "text"), 1))));
        assertFalse(e instanceof RetriableException, e.toString());
    }

    @Test
    void unreachableServerIsRetriedThenFails() throws Exception {
        int closedPort;
        try (ServerSocket s = new ServerSocket(0)) {
            closedPort = s.getLocalPort();
        }
        StubContext context = new StubContext(null);
        GreptimeDBSinkTask task = startTask(
                context,
                GreptimeDBSinkConfig.ENDPOINTS,
                "127.0.0.1:" + closedPort,
                GreptimeDBSinkConfig.TIMESTAMP_FIELD,
                "ts",
                GreptimeDBSinkConfig.MAX_RETRIES,
                "2",
                GreptimeDBSinkConfig.RETRY_BACKOFF_MS,
                "10");
        List<SinkRecord> records = Collections.singletonList(cpu("unreachable", 1L, "h", 1.0, 0));

        assertThrows(RetriableException.class, () -> task.put(records));
        assertThrows(RetriableException.class, () -> task.put(records));
        assertEquals(Arrays.asList(10L, 10L), context.timeouts);

        ConnectException e = assertThrows(ConnectException.class, () -> task.put(records));
        assertFalse(e instanceof RetriableException, e.toString());
        assertTrue(e.getMessage().contains("retries exhausted"), e.getMessage());
    }

    @Test
    void badRecordsGoToErrantReporterAndGoodOnesAreWritten() throws Exception {
        List<SinkRecord> reported = new ArrayList<>();
        GreptimeDBSinkTask task = startTask(
                new StubContext((record, error) -> {
                    assertTrue(error instanceof DataException, error.toString());
                    reported.add(record);
                    return CompletableFuture.completedFuture(null);
                }),
                GreptimeDBSinkConfig.TIMESTAMP_FIELD,
                "ts");

        SinkRecord good = record("dlq", null, map("ts", 1L, "v", 1L), 0);
        SinkRecord missingTs = record("dlq", null, map("v", 2L), 1);
        SinkRecord typeConflict = record("dlq", null, map("ts", 3L, "v", "text"), 2);
        SinkRecord tombstone = record("dlq", null, null, 3);
        task.put(Arrays.asList(good, missingTs, typeConflict, tombstone));

        assertEquals(3, reported.size());
        assertSame(missingTs, reported.get(0));
        assertSame(typeConflict, reported.get(1));
        assertSame(tombstone, reported.get(2));
        assertEquals(Collections.singletonList(Collections.singletonList("1")), GREPTIMEDB.query("SELECT v FROM dlq"));
    }

    @Test
    void badRecordFailsTaskWithoutErrantReporter() {
        GreptimeDBSinkTask task = startTask(new StubContext(null), GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts");
        assertThrows(
                DataException.class,
                () -> task.put(Collections.singletonList(record("no_dlq", null, map("v", 1L), 0))));
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    static final class StubContext implements SinkTaskContext {
        final List<Long> timeouts = new ArrayList<>();
        private final ErrantRecordReporter reporter;

        StubContext(ErrantRecordReporter reporter) {
            this.reporter = reporter;
        }

        @Override
        public ErrantRecordReporter errantRecordReporter() {
            return reporter;
        }

        @Override
        public void timeout(long timeoutMs) {
            timeouts.add(timeoutMs);
        }

        @Override
        public Map<String, String> configs() {
            return Collections.emptyMap();
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {}

        @Override
        public void offset(TopicPartition tp, long offset) {}

        @Override
        public Set<TopicPartition> assignment() {
            return Collections.emptySet();
        }

        @Override
        public void pause(TopicPartition... partitions) {}

        @Override
        public void resume(TopicPartition... partitions) {}

        @Override
        public void requestCommit() {}
    }
}
