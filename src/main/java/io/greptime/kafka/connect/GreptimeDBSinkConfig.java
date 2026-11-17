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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

public class GreptimeDBSinkConfig extends AbstractConfig {

    public static final String ENDPOINTS = "greptimedb.endpoints";
    public static final String DATABASE = "greptimedb.database";
    public static final String USERNAME = "greptimedb.username";
    public static final String PASSWORD = "greptimedb.password";
    public static final String TLS_ENABLED = "greptimedb.tls.enabled";
    public static final String TLS_CA_CERT_PATH = "greptimedb.tls.ca.cert.path";

    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String TIMESTAMP_FIELD = "timestamp.field";
    public static final String TIMESTAMP_PRECISION = "timestamp.precision";
    public static final String TAG_FIELDS = "tag.fields";
    public static final String FIELD_INCLUDE = "field.include";
    public static final String FIELD_EXCLUDE = "field.exclude";
    public static final String BEHAVIOR_ON_NULL_VALUES = "behavior.on.null.values";

    public static final String BATCH_SIZE = "batch.size";
    public static final String MAX_RETRIES = "max.retries";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";

    public static final String TOPIC_PLACEHOLDER = "${topic}";
    /** Time index column name used when the Kafka record timestamp is the time source. */
    public static final String DEFAULT_TIMESTAMP_COLUMN = "greptime_timestamp";

    public enum NullValueBehavior {
        FAIL,
        IGNORE
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ENDPOINTS,
                    Type.LIST,
                    ConfigDef.NO_DEFAULT_VALUE,
                    Importance.HIGH,
                    "GreptimeDB gRPC endpoints as a comma-separated list of host:port, e.g. localhost:4001.")
            .define(DATABASE, Type.STRING, "public", Importance.HIGH, "Target database.")
            .define(USERNAME, Type.STRING, "", Importance.MEDIUM, "Username. Leave empty to disable authentication.")
            .define(PASSWORD, Type.PASSWORD, "", Importance.MEDIUM, "Password.")
            .define(TLS_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM, "Connect to GreptimeDB over TLS.")
            .define(
                    TLS_CA_CERT_PATH,
                    Type.STRING,
                    "",
                    Importance.LOW,
                    "PEM file with the CA certificates used to verify the server. "
                            + "Uses the JVM default trust store when empty.")
            .define(
                    TABLE_NAME_FORMAT,
                    Type.STRING,
                    TOPIC_PLACEHOLDER,
                    new ConfigDef.NonEmptyString(),
                    Importance.HIGH,
                    "Target table name. " + TOPIC_PLACEHOLDER + " is replaced with the record topic.")
            .define(
                    TIMESTAMP_FIELD,
                    Type.STRING,
                    "",
                    Importance.HIGH,
                    "Top-level value field used as the time index column. When empty, the Kafka record "
                            + "timestamp is written to a column named " + DEFAULT_TIMESTAMP_COLUMN + ".")
            .define(
                    TIMESTAMP_PRECISION,
                    Type.STRING,
                    "ms",
                    ConfigDef.ValidString.in("s", "ms", "us", "ns"),
                    Importance.MEDIUM,
                    "Precision of the time index column. Integer timestamp values are interpreted in this unit.")
            .define(TAG_FIELDS, Type.LIST, "", Importance.HIGH, "Top-level value fields written as tag columns.")
            .define(
                    FIELD_INCLUDE,
                    Type.LIST,
                    "",
                    Importance.MEDIUM,
                    "Top-level value fields written as field columns. When empty, all fields other than the "
                            + "timestamp and tag fields are written.")
            .define(FIELD_EXCLUDE, Type.LIST, "", Importance.MEDIUM, "Top-level value fields that are not written.")
            .define(
                    BEHAVIOR_ON_NULL_VALUES,
                    Type.STRING,
                    "fail",
                    ConfigDef.ValidString.in("fail", "ignore"),
                    Importance.MEDIUM,
                    "How to handle records with a null value (tombstones): fail treats them as bad records, "
                            + "ignore skips them.")
            .define(
                    BATCH_SIZE,
                    Type.INT,
                    1000,
                    ConfigDef.Range.atLeast(1),
                    Importance.MEDIUM,
                    "Maximum number of rows per write request.")
            .define(
                    MAX_RETRIES,
                    Type.INT,
                    10,
                    ConfigDef.Range.atLeast(0),
                    Importance.MEDIUM,
                    "Maximum number of consecutive retries of a batch after a retryable write error.")
            .define(
                    RETRY_BACKOFF_MS,
                    Type.LONG,
                    3000L,
                    ConfigDef.Range.atLeast(0),
                    Importance.MEDIUM,
                    "Time to wait before retrying a batch after a retryable write error.");

    private final List<String> endpoints;
    private final String timestampField;
    private final DataType timestampType;
    private final Set<String> tagFields;
    private final Set<String> fieldInclude;
    private final Set<String> fieldExclude;
    private final NullValueBehavior nullValueBehavior;

    public GreptimeDBSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
        this.endpoints = validateEndpoints(getList(ENDPOINTS));
        this.timestampField = getString(TIMESTAMP_FIELD).trim();
        this.timestampType = timestampType(getString(TIMESTAMP_PRECISION));
        this.tagFields = new HashSet<>(getList(TAG_FIELDS));
        this.fieldInclude = new HashSet<>(getList(FIELD_INCLUDE));
        this.fieldExclude = new HashSet<>(getList(FIELD_EXCLUDE));
        this.nullValueBehavior =
                NullValueBehavior.valueOf(getString(BEHAVIOR_ON_NULL_VALUES).toUpperCase(Locale.ROOT));

        String timestampColumn = timestampColumn();
        if (this.tagFields.contains(timestampColumn) || this.fieldInclude.contains(timestampColumn)) {
            throw new ConfigException(String.format(
                    "'%s' is the time index column and cannot be listed in %s or %s",
                    timestampColumn, TAG_FIELDS, FIELD_INCLUDE));
        }
        for (String tag : this.tagFields) {
            if (this.fieldInclude.contains(tag) || this.fieldExclude.contains(tag)) {
                throw new ConfigException(String.format(
                        "Tag field '%s' cannot be listed in %s or %s", tag, FIELD_INCLUDE, FIELD_EXCLUDE));
            }
        }
    }

    private static List<String> validateEndpoints(List<String> endpoints) {
        if (endpoints.isEmpty()) {
            throw new ConfigException(ENDPOINTS, endpoints, "At least one endpoint is required");
        }
        List<String> result = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints) {
            String e = endpoint.trim();
            int idx = e.lastIndexOf(':');
            boolean valid = idx > 0 && idx < e.length() - 1;
            if (valid) {
                try {
                    Integer.parseInt(e.substring(idx + 1));
                } catch (NumberFormatException ex) {
                    valid = false;
                }
            }
            if (!valid) {
                throw new ConfigException(ENDPOINTS, endpoint, "Expected host:port");
            }
            result.add(e);
        }
        return result;
    }

    private static DataType timestampType(String precision) {
        switch (precision.toLowerCase(Locale.ROOT)) {
            case "s":
                return DataType.TimestampSecond;
            case "ms":
                return DataType.TimestampMillisecond;
            case "us":
                return DataType.TimestampMicrosecond;
            case "ns":
                return DataType.TimestampNanosecond;
            default:
                throw new ConfigException(TIMESTAMP_PRECISION, precision, "Expected one of s, ms, us, ns");
        }
    }

    public List<String> endpoints() {
        return this.endpoints;
    }

    public String database() {
        return getString(DATABASE);
    }

    public String username() {
        return getString(USERNAME);
    }

    public Password password() {
        return getPassword(PASSWORD);
    }

    public boolean tlsEnabled() {
        return getBoolean(TLS_ENABLED);
    }

    public String tlsCaCertPath() {
        return getString(TLS_CA_CERT_PATH);
    }

    public String tableNameFormat() {
        return getString(TABLE_NAME_FORMAT);
    }

    /** Empty when the Kafka record timestamp is the time source. */
    public String timestampField() {
        return this.timestampField;
    }

    public String timestampColumn() {
        return this.timestampField.isEmpty() ? DEFAULT_TIMESTAMP_COLUMN : this.timestampField;
    }

    public DataType timestampType() {
        return this.timestampType;
    }

    public Set<String> tagFields() {
        return this.tagFields;
    }

    public Set<String> fieldInclude() {
        return this.fieldInclude;
    }

    public Set<String> fieldExclude() {
        return this.fieldExclude;
    }

    public NullValueBehavior nullValueBehavior() {
        return this.nullValueBehavior;
    }

    public int batchSize() {
        return getInt(BATCH_SIZE);
    }

    public int maxRetries() {
        return getInt(MAX_RETRIES);
    }

    public long retryBackoffMs() {
        return getLong(RETRY_BACKOFF_MS);
    }
}
