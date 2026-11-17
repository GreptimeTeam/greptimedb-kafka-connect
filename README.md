# GreptimeDB Kafka Connect

A Kafka Connect sink connector that writes records from Kafka topics to GreptimeDB tables over gRPC.

- Maps top-level record fields to the time index, tag columns, and field columns.
- Accepts `Struct` values (Avro, Protobuf, JSON Schema, or JSON with `schemas.enable=true`) and schemaless `Map` values (JSON with `schemas.enable=false`).
- Creates tables and adds columns through GreptimeDB's automatic schema creation.
- Writes in batches and retries transient write errors.

## Requirements

- Kafka Connect 2.6 or later. The plugin is compiled for Java 8 and tested with Kafka Connect 3.9.
- GreptimeDB with the gRPC endpoint (default port 4001) reachable from the Connect workers.

## Installation

Build the plugin archive:

```shell
mvn package -DskipTests
```

The build produces `target/greptimedb-kafka-connect-<version>.zip`. Extract it into a directory listed in the worker's `plugin.path`, then restart the worker.

```shell
unzip target/greptimedb-kafka-connect-<version>.zip -d /opt/connectors
```

## Example

```json
{
  "name": "greptimedb-sink",
  "config": {
    "connector.class": "io.greptime.kafka.connect.GreptimeDBSinkConnector",
    "topics": "sensor",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "greptimedb.endpoints": "greptimedb:4001",
    "greptimedb.database": "public",
    "table.name.format": "kafka_${topic}",
    "timestamp.field": "ts",
    "tag.fields": "device",
    "field.exclude": "debug"
  }
}
```

With this configuration, the record

```json
{"ts": 1700000000000, "device": "d1", "temperature": 21.5, "debug": "x"}
```

on topic `sensor` is written to table `kafka_sensor` with time index `ts`, tag `device`, and field `temperature`.

## Configuration

### Connection

| Name | Default | Description |
|---|---|---|
| `greptimedb.endpoints` | (required) | Comma-separated gRPC endpoints, `host:port`. |
| `greptimedb.database` | `public` | Target database. |
| `greptimedb.username` | empty | Username. Authentication is disabled when empty. |
| `greptimedb.password` | empty | Password. |
| `greptimedb.tls.enabled` | `false` | Connect over TLS. |
| `greptimedb.tls.ca.cert.path` | empty | PEM file with the CA certificates used to verify the server. The JVM default trust store is used when empty. |

### Mapping

| Name | Default | Description |
|---|---|---|
| `table.name.format` | `${topic}` | Target table. `${topic}` is replaced with the record topic. |
| `timestamp.field` | empty | Top-level field used as the time index. When empty, the Kafka record timestamp is written to a column named `greptime_timestamp`. |
| `timestamp.precision` | `ms` | Precision of the time index column: `s`, `ms`, `us`, or `ns`. Integer timestamp values are interpreted in this unit. |
| `tag.fields` | empty | Top-level fields written as tag columns. |
| `field.include` | empty | Top-level fields written as field columns. When empty, all fields other than the timestamp and tag fields are written. |
| `field.exclude` | empty | Top-level fields that are not written. |
| `behavior.on.null.values` | `fail` | Records with a null value (tombstones): `fail` treats them as bad records, `ignore` skips them. |

### Writes and retries

| Name | Default | Description |
|---|---|---|
| `batch.size` | `1000` | Maximum number of rows per write request. |
| `max.retries` | `10` | Maximum number of consecutive retries of a batch after a retryable write error. |
| `retry.backoff.ms` | `3000` | Wait time before retrying a batch. |

## Mapping rules

The time index value must be an integer (epoch in `timestamp.precision` units) or a Connect `Timestamp`. Records where it is missing or null are bad records.

Column types come from the Connect schema when the value has one:

| Connect type | GreptimeDB type |
|---|---|
| `INT8`, `INT16`, `INT32`, `INT64` | `Int8`, `Int16`, `Int32`, `Int64` |
| `FLOAT32`, `FLOAT64` | `Float32`, `Float64` |
| `BOOLEAN` | `Boolean` |
| `STRING` | `String` |
| `BYTES` | `Binary` |
| `Decimal` | `Decimal128(38, scale)` |
| `Timestamp` | `TimestampMillisecond` |
| `Date` | `Date` |
| `Time` | `TimeMillisecond` |

For schemaless values, the type comes from the Java value: `Long` to `Int64`, `Double` to `Float64`, `Boolean` to `Boolean`, `String` to `String`. A schemaless field with a null value is left out of the row and stored as null.

Nested values (`STRUCT`, `MAP`, `ARRAY`) are not supported. Records containing them in a written field are bad records. Use `field.exclude` or a transform such as `Flatten` to drop or flatten them.

Within one batch, all records for a table must agree on the type of each column. A record that disagrees with an earlier record in the batch is a bad record. With schemaless JSON, `20` and `20.5` in the same field produce `Int64` and `Float64` and conflict; write floating-point fields with a decimal point, for example `20.0`.

## Delivery and error handling

The connector writes each batch synchronously before Kafka Connect commits offsets, so delivery is at-least-once. Rows replayed after a retry or a task restart overwrite the existing rows with the same tags and time index. Tables created with `append_mode = 'true'` keep the duplicates.

Write errors are classified by the retry hint GreptimeDB returns (v1.2 and later), then by the GreptimeDB status code, then by the gRPC status code. On a retryable error, such as an unreachable server, a busy region, or rate limiting, the task asks Kafka Connect to redeliver the batch after `retry.backoff.ms`. After `max.retries` consecutive retries the task fails. Non-retryable errors, such as a type mismatch with an existing column or an authentication failure, fail the task immediately.

Bad records, which cannot be mapped to a row, are sent to the errant record reporter when `errors.tolerance=all` is set, together with `errors.deadletterqueue.topic.name` if the records should be kept. Otherwise a bad record fails the task.

## Development

```shell
# Unit tests
mvn test

# Unit and integration tests; requires Docker
mvn verify
```

The integration tests use `greptime/greptimedb:v1.3.1` and `apache/kafka:3.9.1`. Override them with `-Dgreptimedb.image=...` and `-Dkafka.image=...`.

## License

Apache License 2.0
