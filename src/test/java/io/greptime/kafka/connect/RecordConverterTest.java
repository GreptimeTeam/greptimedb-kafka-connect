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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class RecordConverterTest {

    static Map<String, String> props(String... kv) {
        Map<String, String> props = new HashMap<>();
        props.put(GreptimeDBSinkConfig.ENDPOINTS, "localhost:4001");
        for (int i = 0; i < kv.length; i += 2) {
            props.put(kv[i], kv[i + 1]);
        }
        return props;
    }

    static RecordConverter converter(String... kv) {
        return new RecordConverter(new GreptimeDBSinkConfig(props(kv)));
    }

    static SinkRecord record(String topic, Schema schema, Object value, Long timestamp) {
        return new SinkRecord(topic, 0, null, null, schema, value, 0L, timestamp, TimestampType.CREATE_TIME);
    }

    static Map<String, ColumnSpec> columns(RecordConverter.Row row) {
        Map<String, ColumnSpec> columns = new LinkedHashMap<>();
        for (ColumnSpec c : row.columns) {
            columns.put(c.name, c);
        }
        return columns;
    }

    static Object value(RecordConverter.Row row, String column) {
        for (int i = 0; i < row.columns.size(); i++) {
            if (row.columns.get(i).name.equals(column)) {
                return row.values.get(i);
            }
        }
        throw new AssertionError("no column " + column);
    }

    @Test
    void structMapsTypesAndValuesTheIngesterAccepts() {
        Schema schema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("host", Schema.STRING_SCHEMA)
                .field("level", Schema.INT8_SCHEMA)
                .field("code", Schema.OPTIONAL_INT16_SCHEMA)
                .field("price", Decimal.schema(2))
                .field("payload", Schema.BYTES_SCHEMA)
                .field("at", Time.SCHEMA)
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("ts", new Date(1_700_000_000_123L))
                .put("host", "h1")
                .put("level", (byte) 3)
                .put("code", null)
                .put("price", new BigDecimal("12.34"))
                .put("payload", ByteBuffer.wrap(new byte[] {1, 2}))
                .put("at", new Date(3_600_000L))
                .put("note", null);

        RecordConverter.Row row = converter(
                        GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts",
                        GreptimeDBSinkConfig.TAG_FIELDS, "host",
                        GreptimeDBSinkConfig.TABLE_NAME_FORMAT, "kafka_${topic}")
                .convert(record("metrics", schema, value, null));

        assertEquals("kafka_metrics", row.table);
        Map<String, ColumnSpec> c = columns(row);
        assertEquals(new ColumnSpec("ts", SemanticType.Timestamp, DataType.TimestampMillisecond), c.get("ts"));
        assertEquals(new ColumnSpec("host", SemanticType.Tag, DataType.String), c.get("host"));
        assertEquals(new ColumnSpec("level", SemanticType.Field, DataType.Int8), c.get("level"));
        assertEquals(new ColumnSpec("price", SemanticType.Field, DataType.Decimal128, 2), c.get("price"));
        assertEquals(new ColumnSpec("at", SemanticType.Field, DataType.TimeMilliSecond), c.get("at"));
        // A null with a schema keeps its column so the type is known.
        assertEquals(new ColumnSpec("note", SemanticType.Field, DataType.String), c.get("note"));

        assertEquals(3, value(row, "level"));
        assertArrayEquals(new byte[] {1, 2}, (byte[]) value(row, "payload"));
        assertEquals(3_600_000L, value(row, "at"));

        // The ingester converts values while adding rows, so this catches values of the wrong Java type.
        TableBatch batch = new TableBatch(row.table);
        batch.add(row, null);
        assertEquals(1, batch.toTable(0, 1).rowCount());
    }

    @Test
    void schemalessMapAppliesIncludeExcludeAndSkipsNulls() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("ts", 1_700_000_000L);
        value.put("region", "us");
        value.put("cpu", 0.5);
        value.put("mem", 1024L);
        value.put("secret", "x");
        value.put("missing", null);

        RecordConverter.Row row = converter(
                        GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts",
                        GreptimeDBSinkConfig.TIMESTAMP_PRECISION, "s",
                        GreptimeDBSinkConfig.TAG_FIELDS, "region",
                        GreptimeDBSinkConfig.FIELD_EXCLUDE, "secret")
                .convert(record("t", null, value, null));

        Map<String, ColumnSpec> c = columns(row);
        assertEquals(Arrays.asList("ts", "region", "cpu", "mem"), new ArrayList<>(c.keySet()));
        assertEquals(DataType.TimestampSecond, c.get("ts").dataType);
        assertEquals(1_700_000_000L, value(row, "ts"));
        assertEquals(DataType.Float64, c.get("cpu").dataType);
        assertEquals(DataType.Int64, c.get("mem").dataType);

        RecordConverter.Row included = converter(
                        GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts",
                        GreptimeDBSinkConfig.TAG_FIELDS, "region",
                        GreptimeDBSinkConfig.FIELD_INCLUDE, "cpu")
                .convert(record("t", null, value, null));
        assertEquals(
                Arrays.asList("ts", "region", "cpu"),
                new ArrayList<>(columns(included).keySet()));
    }

    @Test
    void recordTimestampIsUsedWhenNoTimestampField() {
        RecordConverter c = converter();
        RecordConverter.Row row = c.convert(record("t", null, Collections.singletonMap("v", 1L), 42L));
        assertEquals(
                new ColumnSpec(
                        GreptimeDBSinkConfig.DEFAULT_TIMESTAMP_COLUMN,
                        SemanticType.Timestamp,
                        DataType.TimestampMillisecond),
                row.columns.get(0));
        assertEquals(Instant.ofEpochMilli(42L), row.values.get(0));

        assertThrows(DataException.class, () -> c.convert(record("t", null, Collections.singletonMap("v", 1L), null)));
    }

    @Test
    void rejectsRecordsThatCannotBeMapped() {
        RecordConverter c = converter(GreptimeDBSinkConfig.TIMESTAMP_FIELD, "ts");

        assertThrows(DataException.class, () -> c.convert(record("t", null, Collections.singletonMap("v", 1L), 1L)));

        Map<String, Object> stringTs = new HashMap<>();
        stringTs.put("ts", "2026-01-01T00:00:00Z");
        assertThrows(DataException.class, () -> c.convert(record("t", null, stringTs, null)));

        Map<String, Object> nested = new HashMap<>();
        nested.put("ts", 1L);
        nested.put("labels", Collections.singletonMap("k", "v"));
        DataException e = assertThrows(DataException.class, () -> c.convert(record("t", null, nested, null)));
        assertTrue(e.getMessage().contains("labels"), e.getMessage());

        assertThrows(DataException.class, () -> c.convert(record("t", Schema.STRING_SCHEMA, "text", null)));
    }
}
