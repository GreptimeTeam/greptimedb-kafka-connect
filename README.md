# GreptimeDB Kafka Connect

[![CI](https://github.com/GreptimeTeam/greptimedb-kafka-connect/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-kafka-connect/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Kafka Connect sink connector that writes Kafka records to GreptimeDB over gRPC. You choose which record fields become the time index, tags, and fields. GreptimeDB creates tables and columns automatically.

The plugin targets Java 8 and is tested with Kafka Connect 3.9 and GreptimeDB v1.3.

## Install

1. Build the plugin archive:

   ```shell
   mvn package -DskipTests
   ```

2. Extract `target/greptimedb-kafka-connect-<version>.zip` into a directory listed in the worker's `plugin.path`:

   ```shell
   unzip target/greptimedb-kafka-connect-<version>.zip -d /opt/connectors
   ```

3. Restart the Connect worker.

## Quick start

Create a connector that writes JSON records from topic `sensor` to table `kafka_sensor`:

```shell
curl -X PUT -H 'Content-Type: application/json' http://localhost:8083/connectors/greptimedb-sink/config -d '{
  "connector.class": "io.greptime.kafka.connect.GreptimeDBSinkConnector",
  "topics": "sensor",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "greptimedb.endpoints": "greptimedb:4001",
  "table.name.format": "kafka_${topic}",
  "timestamp.field": "ts",
  "tag.fields": "device",
  "errors.tolerance": "all",
  "errors.deadletterqueue.topic.name": "sensor-dlq",
  "errors.deadletterqueue.topic.replication.factor": "1"
}'
```

Produce a record:

```shell
echo '{"ts": 1700000000000, "device": "d1", "temperature": 21.5}' |
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sensor
```

The record becomes a row with time index `ts`, tag `device`, and field `temperature`:

```shell
curl http://localhost:4000/v1/sql --data-urlencode 'sql=SELECT * FROM kafka_sensor'
```

## Configuration

### Connection

| Name | Default | Description |
|---|---|---|
| `greptimedb.endpoints` | (required) | Comma-separated gRPC endpoints, `host:port`. |
| `greptimedb.database` | `public` | Target database. |
| `greptimedb.username` | Empty | Username. When empty, the connector doesn't authenticate. |
| `greptimedb.password` | Empty | Password. |
| `greptimedb.tls.enabled` | `false` | Connect over TLS. |
| `greptimedb.tls.ca.cert.path` | Empty | PEM file with the CA certificates that verify the server. When empty, the connector uses the JVM default trust store. |

### Mapping

| Name | Default | Description |
|---|---|---|
| `table.name.format` | `${topic}` | Target table. `${topic}` is replaced with the record topic. |
| `timestamp.field` | Empty | Top-level field to use as the time index. When empty, the connector writes the Kafka record timestamp to a column named `greptime_timestamp`. |
| `timestamp.precision` | `ms` | Precision of the time index column: `s`, `ms`, `us`, or `ns`. The connector reads integer timestamp values in this unit. |
| `tag.fields` | Empty | Top-level fields to write as tag columns. |
| `field.include` | Empty | Top-level fields to write as field columns. When empty, the connector writes all fields other than the timestamp and tag fields. |
| `field.exclude` | Empty | Top-level fields to leave out. |
| `behavior.on.null.values` | `fail` | Handling of records with a null value (tombstones). `fail` treats them as bad records. `ignore` skips them. |

### Writes and retries

| Name | Default | Description |
|---|---|---|
| `batch.size` | `1000` | Maximum number of rows per write request. |
| `max.retries` | `10` | Maximum number of consecutive retries of a batch after a retryable write error. |
| `retry.backoff.ms` | `3000` | Time to wait, in milliseconds, before retrying a batch. |

## Notes

- **Choose tags before the first write.** The first write creates the table, and the tags become its primary key. Changing `tag.fields` later doesn't change an existing table, and rows that conflict with it are rejected.
- **The time index must be an integer epoch or a Connect `Timestamp`.** Integers are read in `timestamp.precision` units. String timestamps aren't supported; convert them with the `TimestampConverter` transform.
- **Column types are fixed by the first write.** A later value of a different type is rejected. In schemaless JSON, `20` is an integer and `20.5` is a float, so write floating-point values with a decimal point, such as `20.0`.
- **Nested values aren't supported.** Exclude `STRUCT`, `MAP`, and `ARRAY` fields with `field.exclude`, or flatten structs and maps with the `Flatten` transform.
- **Configure a dead letter queue.** Records that can't be written, such as a record without a time index or a value of the wrong type, are bad records. With `errors.tolerance=all`, they go to the dead letter queue, or are skipped if none is set. With the default `errors.tolerance=none`, a bad record fails the task.
- **Transient errors are retried, never skipped.** When GreptimeDB is unreachable or busy, the connector retries the batch every `retry.backoff.ms`. After `max.retries` consecutive retries, the task fails. Restart it after fixing the cause:

  ```shell
  curl -X POST 'http://localhost:8083/connectors/greptimedb-sink/restart?includeTasks=true&onlyFailed=true'
  ```

- **Delivery is at-least-once.** Replayed rows overwrite rows with the same tags and time index. Tables created with `append_mode = 'true'` keep the duplicates.

### Type mapping

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


## Development

```shell
mvn verify                                 # unit and integration tests, requires Docker
mvn package -DskipTests && e2e/run.sh      # end-to-end test with Kafka, Connect, and GreptimeDB
```

## License

[Apache License 2.0](LICENSE)
