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

package io.greptime.flink.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class GreptimeDynamicTableSinkIT {

    private static final int HTTP_PORT = 4000;
    private static final int RPC_PORT = 4001;
    private static final String DATABASE = "public";
    private static final String TABLE_NAME = "flink_table_sink_it_metrics";
    private static final String GREPTIMEDB_IMAGE = System.getProperty(
            "greptimedb.test.image",
            "greptime/greptimedb:v1.0.1");

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Container
    private static final GenericContainer<?> GREPTIMEDB = new GenericContainer<>(
            DockerImageName.parse(GREPTIMEDB_IMAGE))
            .withExposedPorts(HTTP_PORT, RPC_PORT)
            .withCommand(
                    "standalone",
                    "start",
                    "--http-addr",
                    "0.0.0.0:" + HTTP_PORT,
                    "--rpc-bind-addr",
                    "0.0.0.0:" + RPC_PORT,
                    "--mysql-addr",
                    "0.0.0.0:4002",
                    "--postgres-addr",
                    "0.0.0.0:4003")
            .waitingFor(Wait.forHttp("/health").forPort(HTTP_PORT).forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Test
    void shouldWriteRowsThroughFlinkSqlTableSink() throws Exception {
        executeSql("DROP TABLE IF EXISTS " + TABLE_NAME);
        executeSql("CREATE TABLE " + TABLE_NAME + " ("
                + "ts TIMESTAMP(3) TIME INDEX, "
                + "host STRING, "
                + "region_name STRING, "
                + "cpu_load DOUBLE, "
                + "PRIMARY KEY(host, region_name)"
                + ")");

        TableEnvironment tableEnvironment = TableEnvironment.create(
                EnvironmentSettings.newInstance().inBatchMode().build());
        tableEnvironment.executeSql("CREATE TEMPORARY TABLE metrics ("
                + "host STRING, "
                + "region_name STRING, "
                + "cpu_load DOUBLE, "
                + "ts TIMESTAMP(3) NOT NULL"
                + ") WITH ("
                + "'connector' = 'greptimedb', "
                + "'endpoints' = '" + ingesterEndpoint() + "', "
                + "'database' = '" + DATABASE + "', "
                + "'table' = '" + TABLE_NAME + "', "
                + "'time-index' = 'ts', "
                + "'tags' = 'host,region_name', "
                + "'batch.max-rows' = '2'"
                + ")").await();
        tableEnvironment.executeSql("INSERT INTO metrics VALUES "
                + "('host-a', 'us-west', 0.42, TIMESTAMP '2024-01-02 03:04:05.000'),"
                + "('host-b', 'eu-west', 0.84, TIMESTAMP '2024-01-02 03:04:06.000'),"
                + "('host-c', 'ap-south', 1.26, TIMESTAMP '2024-01-02 03:04:07.000')")
                .await();

        String countResponse = executeSql("SELECT COUNT(*) FROM " + TABLE_NAME);
        assertTrue(countResponse.contains("\"rows\":[[3]]"), countResponse);

        String valueResponse = executeSql("SELECT host, region_name, cpu_load FROM "
                + TABLE_NAME
                + " WHERE host = 'host-b'");
        assertTrue(valueResponse.contains("\"host-b\""), valueResponse);
        assertTrue(valueResponse.contains("\"eu-west\""), valueResponse);
        assertTrue(valueResponse.contains("0.84"), valueResponse);
    }

    private static String ingesterEndpoint() {
        return GREPTIMEDB.getHost() + ":" + GREPTIMEDB.getMappedPort(RPC_PORT);
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
