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
import io.greptime.v1.Common;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeTableSchemaTest {

    @Test
    void shouldRoundTripSchemaMetadata() {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMicrosecond)
                .addTag("host", DataType.String)
                .addField("value", DataType.Float64)
                .addColumn("payload", SemanticType.Field, DataType.Json)
                .build();

        TableSchema roundTripped = GreptimeTableSchema.from(schema).toTableSchema();

        assertEquals(schema.getTableName(), roundTripped.getTableName());
        assertEquals(schema.getColumnNames(), roundTripped.getColumnNames());
        assertEquals(schema.getSemanticTypes(), roundTripped.getSemanticTypes());
        assertEquals(schema.getDataTypes(), roundTripped.getDataTypes());
        assertEquals(schema.getDataTypeExtensions(), roundTripped.getDataTypeExtensions());
    }

    @Test
    void shouldPreserveTimestampMicrosecondAsTimestampType() {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMicrosecond)
                .addField("value", DataType.Float64)
                .build();

        TableSchema roundTripped = GreptimeTableSchema.from(schema).toTableSchema();

        assertEquals(Common.ColumnDataType.TIMESTAMP_MICROSECOND, roundTripped.getDataTypes().get(0));
        assertEquals(Common.SemanticType.TIMESTAMP, roundTripped.getSemanticTypes().get(0));
    }

    @Test
    void shouldPreserveJsonExtension() {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addColumn("payload", SemanticType.Field, DataType.Json)
                .build();

        TableSchema roundTripped = GreptimeTableSchema.from(schema).toTableSchema();
        Common.ColumnDataTypeExtension extension = roundTripped.getDataTypeExtensions().get(1);

        assertEquals(Common.ColumnDataType.JSON, roundTripped.getDataTypes().get(1));
        assertTrue(extension.hasJsonType());
        assertEquals(Common.JsonTypeExtension.JSON_BINARY, extension.getJsonType());
    }

    @Test
    void shouldPreserveDecimalExtension() {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addColumn(
                        "amount",
                        SemanticType.Field,
                        DataType.Decimal128,
                        new DataType.DecimalTypeExtension(20, 4))
                .build();

        TableSchema roundTripped = GreptimeTableSchema.from(schema).toTableSchema();
        Common.DecimalTypeExtension decimalType = roundTripped.getDataTypeExtensions().get(1).getDecimalType();

        assertEquals(Common.ColumnDataType.DECIMAL128, roundTripped.getDataTypes().get(1));
        assertEquals(20, decimalType.getPrecision());
        assertEquals(4, decimalType.getScale());
    }

    @Test
    void shouldSerializeSchemaWithDecimalExtension() throws Exception {
        TableSchema schema = TableSchema.newBuilder("metrics")
                .addTimestamp("ts", DataType.TimestampMillisecond)
                .addColumn(
                        "amount",
                        SemanticType.Field,
                        DataType.Decimal128,
                        new DataType.DecimalTypeExtension(20, 4))
                .build();

        GreptimeTableSchema deserialized = serializeAndDeserialize(GreptimeTableSchema.from(schema));
        Common.DecimalTypeExtension decimalType = deserialized.toTableSchema()
                .getDataTypeExtensions()
                .get(1)
                .getDecimalType();

        assertEquals(20, decimalType.getPrecision());
        assertEquals(4, decimalType.getScale());
    }

    static GreptimeTableSchema serializeAndDeserialize(GreptimeTableSchema schema) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(schema);
        }

        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (GreptimeTableSchema) input.readObject();
        }
    }
}
