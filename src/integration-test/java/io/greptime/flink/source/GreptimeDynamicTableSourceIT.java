/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.greptime.flink.source;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Testcontainers
class GreptimeDynamicTableSourceIT {

    private static final int HTTP_PORT = 4000;
    private static final int MYSQL_PORT = 4002;
    private static final String DATABASE = "public";
    private static final String TABLE_NAME = "flink_table_source_it_metrics";
    private static final String GREPTIMEDB_IMAGE = System.getProperty(
            "greptimedb.test.image",
            "greptime/greptimedb:v1.0.1");

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Container
    private static final GenericContainer<?> GREPTIMEDB = new GenericContainer<>(
            DockerImageName.parse(GREPTIMEDB_IMAGE))
            .withExposedPorts(HTTP_PORT, MYSQL_PORT)
            .withCommand(
                    "standalone",
                    "start",
                    "--http-addr",
                    "0.0.0.0:" + HTTP_PORT,
                    "--rpc-bind-addr",
                    "0.0.0.0:4001",
                    "--mysql-addr",
                    "0.0.0.0:" + MYSQL_PORT,
                    "--postgres-addr",
                    "0.0.0.0:4003")
            .waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Test
    void shouldReadRowsThroughFlinkSqlTableSource() throws Exception {
        executeSql("DROP TABLE IF EXISTS " + TABLE_NAME);
        executeSql("CREATE TABLE " + TABLE_NAME + " ("
                + "metric_id INT, "
                + "active BOOLEAN, "
                + "big_value BIGINT, "
                + "float_value FLOAT, "
                + "double_value DOUBLE, "
                + "host STRING, "
                + "payload VARBINARY, "
                + "amount DECIMAL(10, 2), "
                + "observed_on DATE, "
                + "nullable_text STRING, "
                + "ts TIMESTAMP(3) TIME INDEX, "
                + "PRIMARY KEY(metric_id)"
                + ")");
        insertRows();

        TableEnvironment tableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.executeSql("CREATE TEMPORARY TABLE metrics ("
                + "metric_id INT, "
                + "active BOOLEAN, "
                + "big_value BIGINT, "
                + "float_value FLOAT, "
                + "double_value DOUBLE, "
                + "host STRING, "
                + "payload VARBINARY(16), "
                + "amount DECIMAL(10, 2), "
                + "observed_on DATE, "
                + "nullable_text STRING, "
                + "ts TIMESTAMP(3)"
                + ") WITH ("
                + "'connector' = 'greptimedb', "
                + "'query.jdbc-url' = '" + jdbcUrl() + "', "
                + "'database' = '" + DATABASE + "', "
                + "'table' = '" + TABLE_NAME + "', "
                + "'query.fetch-size' = '2'"
                + ")").await();

        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> result = tableEnvironment
                .executeSql("SELECT metric_id, active, big_value, float_value, double_value, "
                        + "host, payload, amount, observed_on, nullable_text, ts "
                        + "FROM metrics ORDER BY metric_id")
                .collect()) {
            result.forEachRemaining(rows::add);
        }

        assertEquals(2, rows.size());
        Row first = rows.get(0);
        assertEquals(1, first.getField(0));
        assertEquals(true, first.getField(1));
        assertEquals(1_234_567_890_123L, first.getField(2));
        assertEquals(1.25f, first.getField(3));
        assertEquals(2.5d, first.getField(4));
        assertEquals("host-a", first.getField(5));
        assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) first.getField(6));
        assertEquals(new BigDecimal("12.34"), first.getField(7));
        assertEquals(LocalDate.of(2026, 7, 9), first.getField(8));
        assertEquals("filled", first.getField(9));
        assertEquals(
                LocalDateTime.of(2026, 7, 9, 1, 2, 3, 123_000_000),
                first.getField(10));

        Row second = rows.get(1);
        assertEquals(2, second.getField(0));
        assertNull(second.getField(1));
        assertEquals(9L, second.getField(2));
        assertNull(second.getField(3));
        assertEquals(5.0d, second.getField(4));
        assertEquals("host-b", second.getField(5));
        assertNull(second.getField(6));
        assertNull(second.getField(7));
        assertNull(second.getField(8));
        assertNull(second.getField(9));
        assertEquals(
                LocalDateTime.of(2026, 7, 10, 4, 5, 6, 456_000_000),
                second.getField(10));
    }

    private static void insertRows() throws Exception {
        String sql = "INSERT INTO " + TABLE_NAME
                + " (metric_id, active, big_value, float_value, double_value, host, payload, amount, "
                + "observed_on, nullable_text, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection connection = DriverManager.getConnection(jdbcUrl());
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setBoolean(2, true);
            statement.setLong(3, 1_234_567_890_123L);
            statement.setFloat(4, 1.25f);
            statement.setDouble(5, 2.5d);
            statement.setString(6, "host-a");
            statement.setBytes(7, new byte[] {1, 2, 3});
            statement.setBigDecimal(8, new BigDecimal("12.34"));
            statement.setDate(9, Date.valueOf("2026-07-09"));
            statement.setString(10, "filled");
            statement.setTimestamp(11, Timestamp.valueOf("2026-07-09 01:02:03.123"));
            statement.executeUpdate();

            statement.setInt(1, 2);
            statement.setNull(2, Types.BOOLEAN);
            statement.setLong(3, 9L);
            statement.setNull(4, Types.FLOAT);
            statement.setDouble(5, 5.0d);
            statement.setString(6, "host-b");
            statement.setNull(7, Types.VARBINARY);
            statement.setNull(8, Types.DECIMAL);
            statement.setNull(9, Types.DATE);
            statement.setNull(10, Types.VARCHAR);
            statement.setTimestamp(11, Timestamp.valueOf("2026-07-10 04:05:06.456"));
            statement.executeUpdate();
        }
    }

    private static String jdbcUrl() {
        return "jdbc:mysql://" + GREPTIMEDB.getHost() + ":"
                + GREPTIMEDB.getMappedPort(MYSQL_PORT) + "/" + DATABASE + "?useSSL=false";
    }

    private static String executeSql(String sql) throws IOException, InterruptedException {
        String body = "sql=" + URLEncoder.encode(sql, StandardCharsets.UTF_8);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + GREPTIMEDB.getHost() + ":"
                        + GREPTIMEDB.getMappedPort(HTTP_PORT) + "/v1/sql?db=" + DATABASE))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        return response.body();
    }
}
