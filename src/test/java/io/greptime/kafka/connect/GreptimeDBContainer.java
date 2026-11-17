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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

class GreptimeDBContainer extends GenericContainer<GreptimeDBContainer> {

    static final int HTTP_PORT = 4000;
    static final int GRPC_PORT = 4001;
    static final int POSTGRES_PORT = 4003;

    GreptimeDBContainer() {
        super(System.getProperty("greptimedb.image", "greptime/greptimedb:v1.3.1"));
        withCommand(
                "standalone",
                "start",
                "--http-addr=0.0.0.0:" + HTTP_PORT,
                "--grpc-bind-addr=0.0.0.0:" + GRPC_PORT,
                "--postgres-addr=0.0.0.0:" + POSTGRES_PORT);
        withExposedPorts(HTTP_PORT, GRPC_PORT, POSTGRES_PORT);
        waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT));
    }

    String grpcEndpoint() {
        return getHost() + ":" + getMappedPort(GRPC_PORT);
    }

    /** Runs a query and returns each row as a list of column values rendered with {@code getString}. */
    List<List<String>> query(String sql) throws SQLException {
        String url = "jdbc:postgresql://" + getHost() + ":" + getMappedPort(POSTGRES_PORT) + "/public";
        try (Connection conn = DriverManager.getConnection(url, "greptime", "");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            List<List<String>> rows = new ArrayList<>();
            while (rs.next()) {
                List<String> row = new ArrayList<>(n);
                for (int i = 1; i <= n; i++) {
                    row.add(rs.getString(i));
                }
                rows.add(row);
            }
            return rows;
        }
    }
}
