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
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.MountableFile;

/** Runs the packaged plugin zip in a real Connect worker: Kafka topic, Connect, GreptimeDB. */
class ConnectPluginIT {

    private static final String KAFKA_IMAGE = System.getProperty("kafka.image", "apache/kafka:3.9.1");
    private static final int CONNECT_PORT = 8083;

    @TempDir
    Path tmp;

    @Test
    void jsonRecordsFlowFromTopicToTable() throws Exception {
        Path pluginDir = unzip(Paths.get(System.getProperty("plugin.zip")), tmp.resolve("plugins"));

        try (Network network = Network.newNetwork();
                KafkaContainer kafka =
                        new KafkaContainer(KAFKA_IMAGE).withNetwork(network).withListener("kafka:19092");
                GreptimeDBContainer greptimedb =
                        new GreptimeDBContainer().withNetwork(network).withNetworkAliases("greptimedb");
                GenericContainer<?> connect = new GenericContainer<>(KAFKA_IMAGE)
                        .withNetwork(network)
                        .withCopyFileToContainer(MountableFile.forHostPath(pluginDir), "/opt/connectors")
                        .withCopyToContainer(
                                org.testcontainers.images.builder.Transferable.of(workerConfig()),
                                "/tmp/connect.properties")
                        .withCommand("/opt/kafka/bin/connect-distributed.sh", "/tmp/connect.properties")
                        .withExposedPorts(CONNECT_PORT)
                        .waitingFor(Wait.forHttp("/connector-plugins").forPort(CONNECT_PORT))) {
            kafka.start();
            greptimedb.start();
            connect.start();

            String connectUrl = "http://" + connect.getHost() + ":" + connect.getMappedPort(CONNECT_PORT);
            String plugins = http("GET", connectUrl + "/connector-plugins", null);
            assertTrue(plugins.contains(GreptimeDBSinkConnector.class.getName()), plugins);

            http(
                    "POST",
                    connectUrl + "/connectors",
                    "{\"name\":\"greptimedb-sink\",\"config\":{"
                            + "\"connector.class\":\"" + GreptimeDBSinkConnector.class.getName() + "\","
                            + "\"topics\":\"sensor\","
                            + "\"key.converter\":\"org.apache.kafka.connect.storage.StringConverter\","
                            + "\"value.converter\":\"org.apache.kafka.connect.json.JsonConverter\","
                            + "\"value.converter.schemas.enable\":\"false\","
                            + "\"greptimedb.endpoints\":\"greptimedb:" + GreptimeDBContainer.GRPC_PORT + "\","
                            + "\"table.name.format\":\"kafka_${topic}\","
                            + "\"timestamp.field\":\"ts\","
                            + "\"tag.fields\":\"device\","
                            + "\"field.exclude\":\"debug\","
                            + "\"batch.size\":\"3\"}}");

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                for (int i = 0; i < 10; i++) {
                    String json = String.format(
                            "{\"ts\":%d,\"device\":\"d%d\",\"temperature\":%d.5,\"debug\":\"x\"}",
                            1_700_000_000_000L + i, i % 3, 20 + i);
                    producer.send(new ProducerRecord<>("sensor", json)).get();
                }
            }

            List<List<String>> count = Collections.emptyList();
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                try {
                    count = greptimedb.query("SELECT count(*) FROM kafka_sensor");
                    if ("10".equals(count.get(0).get(0))) {
                        break;
                    }
                } catch (java.sql.SQLException e) {
                    // The table does not exist until the first batch is written.
                }
                Thread.sleep(500);
            }
            assertEquals(Collections.singletonList(Collections.singletonList("10")), count);
            assertEquals(
                    Arrays.asList(
                            Arrays.asList("device", "TAG"),
                            Arrays.asList("temperature", "FIELD"),
                            Arrays.asList("ts", "TIMESTAMP")),
                    // Schemaless JSON maps have no field order, so neither do the created columns.
                    greptimedb.query("SELECT column_name, semantic_type FROM information_schema.columns "
                            + "WHERE table_name = 'kafka_sensor' ORDER BY column_name"));
            assertEquals(
                    Collections.singletonList(Arrays.asList("d0", "29.5")),
                    greptimedb.query("SELECT device, temperature FROM kafka_sensor ORDER BY ts DESC LIMIT 1"));

            String status = http("GET", connectUrl + "/connectors/greptimedb-sink/status", null);
            assertFalse(status.contains("FAILED"), status);
        }
    }

    private static String workerConfig() {
        return String.join(
                "\n",
                "bootstrap.servers=kafka:19092",
                "group.id=connect-it",
                "key.converter=org.apache.kafka.connect.storage.StringConverter",
                "value.converter=org.apache.kafka.connect.json.JsonConverter",
                "config.storage.topic=connect-configs",
                "offset.storage.topic=connect-offsets",
                "status.storage.topic=connect-status",
                "config.storage.replication.factor=1",
                "offset.storage.replication.factor=1",
                "status.storage.replication.factor=1",
                "offset.flush.interval.ms=1000",
                "listeners=http://0.0.0.0:" + CONNECT_PORT,
                "plugin.path=/opt/connectors",
                "");
    }

    private static Path unzip(Path zip, Path target) throws IOException {
        try (ZipInputStream in = new ZipInputStream(Files.newInputStream(zip))) {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null) {
                Path out = target.resolve(entry.getName()).normalize();
                if (!out.startsWith(target)) {
                    throw new IOException("Bad zip entry " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(out);
                } else {
                    Files.createDirectories(out.getParent());
                    Files.copy(in, out);
                }
            }
        }
        return target;
    }

    private static String http(String method, String url, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod(method);
        if (body != null) {
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            try (OutputStream out = conn.getOutputStream()) {
                out.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }
        int code = conn.getResponseCode();
        InputStream in = code < 400 ? conn.getInputStream() : conn.getErrorStream();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        if (in != null) {
            try (InputStream i = in) {
                byte[] b = new byte[4096];
                int n;
                while ((n = i.read(b)) > 0) {
                    buf.write(b, 0, n);
                }
            }
        }
        String response = new String(buf.toByteArray(), StandardCharsets.UTF_8);
        if (code >= 400) {
            throw new IOException(method + " " + url + " -> " + code + ": " + response);
        }
        return response;
    }
}
