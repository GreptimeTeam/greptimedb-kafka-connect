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

import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/** Maps a record value (a {@link Struct} or a schemaless {@link Map}) to one GreptimeDB row. */
final class RecordConverter {

    private final String tableNameFormat;
    private final String timestampField;
    private final String timestampColumn;
    private final DataType timestampType;
    private final Set<String> tagFields;
    private final Set<String> fieldInclude;
    private final Set<String> fieldExclude;

    RecordConverter(GreptimeDBSinkConfig config) {
        this.tableNameFormat = config.tableNameFormat();
        this.timestampField = config.timestampField();
        this.timestampColumn = config.timestampColumn();
        this.timestampType = config.timestampType();
        this.tagFields = config.tagFields();
        this.fieldInclude = config.fieldInclude();
        this.fieldExclude = config.fieldExclude();
    }

    /** Throws {@link DataException} when the record cannot be mapped. */
    Row convert(SinkRecord record) {
        String table = this.tableNameFormat.replace(GreptimeDBSinkConfig.TOPIC_PLACEHOLDER, record.topic());
        Row row = new Row(table);

        Map<String, Object> values = new LinkedHashMap<>();
        Map<String, Schema> schemas = new LinkedHashMap<>();
        Object value = record.value();
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                values.put(field.name(), struct.get(field));
                schemas.put(field.name(), field.schema());
            }
        } else if (value instanceof Map) {
            for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
                if (!(e.getKey() instanceof String)) {
                    throw new DataException("Map value keys must be strings, got " + typeName(e.getKey()));
                }
                values.put((String) e.getKey(), e.getValue());
            }
        } else {
            throw new DataException("Record value must be a Struct or a Map, got " + typeName(value));
        }

        row.add(
                new ColumnSpec(this.timestampColumn, SemanticType.Timestamp, this.timestampType),
                timestamp(record, values));

        for (Map.Entry<String, Object> e : values.entrySet()) {
            String name = e.getKey();
            if (name.equals(this.timestampField)) {
                continue;
            }
            SemanticType semanticType;
            if (this.tagFields.contains(name)) {
                semanticType = SemanticType.Tag;
            } else if (this.fieldExclude.contains(name)
                    || (!this.fieldInclude.isEmpty() && !this.fieldInclude.contains(name))) {
                continue;
            } else {
                semanticType = SemanticType.Field;
            }
            if (name.equals(this.timestampColumn)) {
                throw new DataException("Field '" + name + "' collides with the time index column");
            }

            Schema schema = schemas.get(name);
            Object v = e.getValue();
            if (schema == null && v == null) {
                // A schemaless null carries no type; leaving the column out writes the same null.
                continue;
            }
            try {
                ColumnSpec column =
                        schema != null ? fromSchema(name, semanticType, schema) : fromJava(name, semanticType, v);
                row.add(column, normalize(column, v));
            } catch (DataException ex) {
                throw new DataException("Field '" + name + "': " + ex.getMessage(), ex);
            }
        }
        return row;
    }

    private Object timestamp(SinkRecord record, Map<String, Object> values) {
        if (this.timestampField.isEmpty()) {
            Long ts = record.timestamp();
            if (ts == null) {
                throw new DataException(
                        "Record has no timestamp and " + GreptimeDBSinkConfig.TIMESTAMP_FIELD + " is not set");
            }
            return fromEpochMillis(ts);
        }
        Object ts = values.get(this.timestampField);
        if (ts == null) {
            throw new DataException("Timestamp field '" + this.timestampField + "' is missing or null");
        }
        // java.util.Date covers the Connect Timestamp and Date logical types.
        if (ts instanceof Date) {
            return fromEpochMillis(((Date) ts).getTime());
        }
        if (ts instanceof Long || ts instanceof Integer || ts instanceof Short || ts instanceof Byte) {
            return ((Number) ts).longValue();
        }
        throw new DataException("Timestamp field '" + this.timestampField
                + "' must be an integer epoch value or a Connect Timestamp, got " + typeName(ts));
    }

    /**
     * Converts here rather than in the ingester, so a value outside the range of the precision (years
     * 1677 to 2262 for nanoseconds) is a bad record instead of an exception that fails the whole batch.
     */
    private long fromEpochMillis(long millis) {
        try {
            switch (this.timestampType) {
                case TimestampSecond:
                    return Math.floorDiv(millis, 1000L);
                case TimestampMillisecond:
                    return millis;
                case TimestampMicrosecond:
                    return Math.multiplyExact(millis, 1000L);
                case TimestampNanosecond:
                    return Math.multiplyExact(millis, 1_000_000L);
                default:
                    throw new IllegalStateException("Unexpected time index type " + this.timestampType);
            }
        } catch (ArithmeticException e) {
            throw new DataException(
                    "Timestamp " + Instant.ofEpochMilli(millis) + " is out of range for " + this.timestampType, e);
        }
    }

    private static ColumnSpec fromSchema(String name, SemanticType semanticType, Schema schema) {
        String logical = schema.name();
        if (logical != null) {
            switch (logical) {
                case Decimal.LOGICAL_NAME:
                    return new ColumnSpec(name, semanticType, DataType.Decimal128, decimalScale(schema));
                case Timestamp.LOGICAL_NAME:
                    return new ColumnSpec(name, semanticType, DataType.TimestampMillisecond);
                case org.apache.kafka.connect.data.Date.LOGICAL_NAME:
                    return new ColumnSpec(name, semanticType, DataType.Date);
                case Time.LOGICAL_NAME:
                    return new ColumnSpec(name, semanticType, DataType.TimeMilliSecond);
                default:
                    // Unknown logical types are written as their physical type.
            }
        }
        switch (schema.type()) {
            case INT8:
                return new ColumnSpec(name, semanticType, DataType.Int8);
            case INT16:
                return new ColumnSpec(name, semanticType, DataType.Int16);
            case INT32:
                return new ColumnSpec(name, semanticType, DataType.Int32);
            case INT64:
                return new ColumnSpec(name, semanticType, DataType.Int64);
            case FLOAT32:
                return new ColumnSpec(name, semanticType, DataType.Float32);
            case FLOAT64:
                return new ColumnSpec(name, semanticType, DataType.Float64);
            case BOOLEAN:
                return new ColumnSpec(name, semanticType, DataType.Bool);
            case STRING:
                return new ColumnSpec(name, semanticType, DataType.String);
            case BYTES:
                return new ColumnSpec(name, semanticType, DataType.Binary);
            default:
                throw new DataException("Unsupported schema type " + schema.type());
        }
    }

    private static int decimalScale(Schema schema) {
        String scale = schema.parameters() == null ? null : schema.parameters().get(Decimal.SCALE_FIELD);
        if (scale == null) {
            throw new DataException("Decimal schema has no scale parameter");
        }
        return Integer.parseInt(scale);
    }

    private static ColumnSpec fromJava(String name, SemanticType semanticType, Object v) {
        if (v instanceof String) {
            return new ColumnSpec(name, semanticType, DataType.String);
        }
        if (v instanceof Long) {
            return new ColumnSpec(name, semanticType, DataType.Int64);
        }
        if (v instanceof Double) {
            return new ColumnSpec(name, semanticType, DataType.Float64);
        }
        if (v instanceof Boolean) {
            return new ColumnSpec(name, semanticType, DataType.Bool);
        }
        if (v instanceof Integer) {
            return new ColumnSpec(name, semanticType, DataType.Int32);
        }
        if (v instanceof Short) {
            return new ColumnSpec(name, semanticType, DataType.Int16);
        }
        if (v instanceof Byte) {
            return new ColumnSpec(name, semanticType, DataType.Int8);
        }
        if (v instanceof Float) {
            return new ColumnSpec(name, semanticType, DataType.Float32);
        }
        if (v instanceof byte[] || v instanceof ByteBuffer) {
            return new ColumnSpec(name, semanticType, DataType.Binary);
        }
        if (v instanceof Date) {
            return new ColumnSpec(name, semanticType, DataType.TimestampMillisecond);
        }
        throw new DataException("Unsupported value type " + typeName(v));
    }

    /** Converts a Connect value into the Java type the ingester expects for the column type. */
    private static Object normalize(ColumnSpec column, Object v) {
        if (v == null) {
            return null;
        }
        switch (column.dataType) {
            case Int8:
            case Int16:
                // The ingester casts Int8/Int16 values with (int).
                return ((Number) v).intValue();
            case Binary:
                return v instanceof ByteBuffer ? toBytes((ByteBuffer) v) : v;
            case TimeMilliSecond:
                // Connect Time is a java.util.Date holding milliseconds since midnight.
                return ((Date) v).getTime();
            case Decimal128:
                BigDecimal d = (BigDecimal) v;
                if (d.precision() > ColumnSpec.DECIMAL_PRECISION) {
                    throw new DataException(
                            "Decimal value " + d + " exceeds precision " + ColumnSpec.DECIMAL_PRECISION);
                }
                return d;
            default:
                return v;
        }
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer b = buffer.duplicate();
        byte[] bytes = new byte[b.remaining()];
        b.get(bytes);
        return bytes;
    }

    private static String typeName(Object v) {
        return v == null ? "null" : v.getClass().getName();
    }

    /** One converted record: columns and values in the same order. */
    static final class Row {
        final String table;
        final List<ColumnSpec> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();

        Row(String table) {
            this.table = table;
        }

        void add(ColumnSpec column, Object value) {
            this.columns.add(column);
            this.values.add(value);
        }
    }
}
