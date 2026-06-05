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

package io.greptime.flink.sink;

import io.greptime.models.DataType;
import io.greptime.models.TableSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
class GreptimeSinkIT {

    private static final int HTTP_PORT = 4000;
    private static final int RPC_PORT = 4001;
    private static final String DATABASE = "public";
    private static final String TABLE_NAME = "flink_sink_it_metrics";
    private static final String GREPTIMEDB_IMAGE = System.getProperty(
            "greptimedb.test.image",
            "greptime/greptimedb:latest");

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
    void shouldWriteRecordsIntoGreptimeDbThroughFlinkSink() throws Exception {
        executeSql("DROP TABLE IF EXISTS " + TABLE_NAME);
        executeSql("CREATE TABLE " + TABLE_NAME + " ("
                + "ts TIMESTAMP(3) TIME INDEX, "
                + "host STRING, "
                + "cpu_load DOUBLE, "
                + "PRIMARY KEY(host)"
                + ")");

        TableSchema tableSchema = TableSchema.newBuilder(TABLE_NAME)
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addTag("host", DataType.String)
                .addField("cpu_load", DataType.Float64)
                .build();

        GreptimeSink<String> sink = GreptimeSink.<String>builder()
                .endpoint(ingesterEndpoint())
                .database(DATABASE)
                .tableSchema(tableSchema)
                .batchSize(2)
                .recordSerializer(GreptimeSinkIT::serializeMetric)
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromData(
                "1700000000000,host-a,0.42",
                "1700000001000,host-b,0.84",
                "1700000002000,host-c,1.26")
                .sinkTo(sink);
        env.execute("GreptimeDB sink integration test");

        String countResponse = executeSql("SELECT COUNT(*) FROM " + TABLE_NAME);
        assertTrue(countResponse.contains("\"rows\":[[3]]"), countResponse);

        String valueResponse = executeSql("SELECT host, cpu_load FROM " + TABLE_NAME + " WHERE host = 'host-b'");
        assertTrue(valueResponse.contains("\"host-b\""), valueResponse);
        assertTrue(valueResponse.contains("0.84"), valueResponse);
    }

    static Object[] serializeMetric(String value) {
        String[] parts = value.split(",", -1);
        assertEquals(3, parts.length);
        return new Object[] {
                Long.parseLong(parts[0]),
                parts[1],
                Double.parseDouble(parts[2])
        };
    }

    static String ingesterEndpoint() {
        return GREPTIMEDB.getHost() + ":" + GREPTIMEDB.getMappedPort(RPC_PORT);
    }

    static String executeSql(String sql) throws IOException, InterruptedException {
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
