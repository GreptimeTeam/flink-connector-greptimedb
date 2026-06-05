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
import io.greptime.models.SemanticType;
import io.greptime.models.TableSchema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GreptimeSinkBuilderTest {

    private static final TableSchema TABLE_SCHEMA = TableSchema.newBuilder("metrics")
            .addTimestamp("ts", DataType.TimestampMillisecond)
            .addTag("host", DataType.String)
            .addField("value", DataType.Float64)
            .build();

    @Test
    void shouldBuildSinkWithRequiredFields() {
        GreptimeSink<String> sink = GreptimeSink.<String>builder()
                .endpoint("127.0.0.1:4001")
                .tableSchema(TABLE_SCHEMA)
                .recordSerializer(value -> new Object[] { 1L, "host-1", Double.parseDouble(value) })
                .build();

        assertNotNull(sink);
    }

    @Test
    void shouldBuildSerializableSink() throws Exception {
        GreptimeSink<String> sink = GreptimeSink.<String>builder()
                .endpoint("127.0.0.1:4001")
                .tableSchema(TABLE_SCHEMA)
                .recordSerializer(value -> new Object[] { 1L, "host-1", Double.parseDouble(value) })
                .build();

        try (ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream())) {
            output.writeObject(sink);
        }
    }

    @Test
    void shouldBuildSerializableSinkWithComplexSchema() throws Exception {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMicrosecond)
                .addTag("host", DataType.String)
                .addColumn("payload", SemanticType.Field, DataType.Json)
                .addColumn(
                        "amount",
                        SemanticType.Field,
                        DataType.Decimal128,
                        new DataType.DecimalTypeExtension(20, 4))
                .build();
        GreptimeSink<String> sink = GreptimeSink.<String>builder()
                .endpoint("127.0.0.1:4001")
                .tableSchema(schema)
                .recordSerializer(value -> new Object[] { 1L, "host-1", "{\"value\":1}", "1.23" })
                .build();

        try (ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream())) {
            output.writeObject(sink);
        }
    }

    @Test
    void shouldRejectMissingEndpoint() {
        assertThrows(IllegalStateException.class, () -> GreptimeSink.<String>builder()
                .tableSchema(TABLE_SCHEMA)
                .recordSerializer(value -> new Object[] { 1L, "host-1", Double.parseDouble(value) })
                .build());
    }

    @Test
    void shouldRejectInvalidBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> GreptimeSink.<String>builder().batchSize(0));
    }
}
