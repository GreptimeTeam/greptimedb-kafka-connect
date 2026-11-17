# GreptimeDB Kafka Connect

[![CI](https://github.com/GreptimeTeam/greptimedb-kafka-connect/actions/workflows/ci.yml/badge.svg)](https://github.com/GreptimeTeam/greptimedb-kafka-connect/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A Kafka Connect sink connector that writes Kafka records to GreptimeDB over gRPC. You choose which record fields become the time index, tags, and fields, as defined in the [GreptimeDB data model](https://docs.greptime.com/user-guide/concepts/data-model/). GreptimeDB creates tables and columns automatically.

The plugin targets Java 8 and is tested with Kafka Connect 3.9 and GreptimeDB v1.3.

## Install

1. Download `greptimedb-kafka-connect-<version>.zip` from [Releases](https://github.com/GreptimeTeam/greptimedb-kafka-connect/releases), or build it from source with `mvn package -DskipTests`, which writes it to `target/`.
2. Extract the archive into a directory listed in the worker's `plugin.path`:

   ```shell
   unzip path/to/greptimedb-kafka-connect-<version>.zip -d /opt/connectors
   ```

3. Restart the Connect worker.

## Quick start

This walkthrough runs Kafka, a Connect worker, and GreptimeDB locally with Docker Compose. Run the commands from a clone of this repository.

1. Build the plugin, put it where the Connect worker loads it, and start the services:

   ```shell
   mvn package -DskipTests
   mkdir -p e2e/.plugins
   unzip target/greptimedb-kafka-connect-*.zip -d e2e/.plugins
   docker compose -f e2e/docker-compose.yml up -d
   ```

   The Connect REST API listens on `localhost:18083`, and the GreptimeDB HTTP API on `localhost:14000`. Connect is ready when `curl -s localhost:18083/connector-plugins` lists `GreptimeDBSinkConnector`.

2. Create a connector that writes JSON records from topic `sensor` to table `kafka_sensor`:

   ```shell
   curl -X PUT -H 'Content-Type: application/json' http://localhost:18083/connectors/greptimedb-sink/config -d '{
     "connector.class": "io.greptime.kafka.connect.GreptimeDBSinkConnector",
     "topics": "sensor",
     "value.converter": "org.apache.kafka.connect.json.JsonConverter",
     "value.converter.schemas.enable": "false",
     "greptimedb.endpoints": "greptimedb:4001",
     "table.name.format": "kafka_${topic}",
     "timestamp.field": "ts",
     "tag.fields": "device"
   }'
   ```

3. Produce a record:

   ```shell
   echo '{"ts": 1700000000000, "device": "d1", "temperature": 21.5}' |
     docker compose -f e2e/docker-compose.yml exec -T kafka \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic sensor
   ```

4. Query the table. The record is a row with time index `ts`, tag `device`, and field `temperature`:

   ```shell
   curl http://localhost:14000/v1/sql --data-urlencode 'sql=SELECT * FROM kafka_sensor'
   ```

   The connector writes the record asynchronously. If the query reports that the table doesn't exist or returns no rows, wait a few seconds and run it again.

5. Stop the services:

   ```shell
   docker compose -f e2e/docker-compose.yml down -v
   ```

## Use with Avro and Schema Registry

With Avro, Protobuf, or JSON Schema, column types come from the record schema. See [Type mapping](#type-mapping). Set the converter as for any other connector; the GreptimeDB settings stay the same:

```json
{
  "connector.class": "io.greptime.kafka.connect.GreptimeDBSinkConnector",
  "topics": "sensor",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081",
  "greptimedb.endpoints": "greptimedb:4001",
  "timestamp.field": "ts",
  "tag.fields": "device"
}
```

The Connect worker must have the Confluent Avro converter installed.

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


## Performance tuning

Each task writes one batch at a time and waits for GreptimeDB to acknowledge it before writing the next.

- **Scale with `tasks.max`.** Throughput grows with the number of tasks, up to the number of topic partitions. Create the topic with enough partitions for the parallelism you need.
- **Keep Connect workers close to GreptimeDB.** Every batch waits for one round trip, so network latency directly limits the throughput of a task.
- **Tune batch size last.** A batch holds at most `max.poll.records` records (500 by default). Raising it and `batch.size` gives a smaller gain than adding tasks. To raise it for this connector only, set `consumer.override.max.poll.records`. This works unless the worker restricts `connector.client.config.override.policy`, which defaults to `All`.

## Development

```shell
mvn verify                                 # unit and integration tests, requires Docker
mvn package -DskipTests && e2e/run.sh      # end-to-end test with Kafka, Connect, and GreptimeDB
```

## License

[Apache License 2.0](LICENSE)
