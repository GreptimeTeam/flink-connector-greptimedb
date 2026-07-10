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

import io.greptime.models.TableSchema;
import io.greptime.v1.Common;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeTableSchemaConverterTest {

    @Test
    void shouldConvertPhysicalSchemaToIngesterSchema() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("host", DataTypes.STRING()),
                Column.physical("region", DataTypes.STRING()),
                Column.physical("usage", DataTypes.DOUBLE()),
                Column.physical("amount", DataTypes.DECIMAL(20, 4)),
                Column.physical("ts", DataTypes.TIMESTAMP(3).notNull())));
        GreptimeTableSinkOptions options = options("metrics", "ts", List.of("host", "region"));

        TableSchema tableSchema = GreptimeTableSchemaConverter.convert(schema, options);

        assertEquals("metrics", tableSchema.getTableName());
        assertEquals(List.of("host", "region", "usage", "amount", "ts"), tableSchema.getColumnNames());
        assertEquals(
                List.of(
                        Common.SemanticType.TAG,
                        Common.SemanticType.TAG,
                        Common.SemanticType.FIELD,
                        Common.SemanticType.FIELD,
                        Common.SemanticType.TIMESTAMP),
                tableSchema.getSemanticTypes());
        assertEquals(
                List.of(
                        Common.ColumnDataType.STRING,
                        Common.ColumnDataType.STRING,
                        Common.ColumnDataType.FLOAT64,
                        Common.ColumnDataType.DECIMAL128,
                        Common.ColumnDataType.TIMESTAMP_MILLISECOND),
                tableSchema.getDataTypes());

        Common.DecimalTypeExtension decimalType =
                tableSchema.getDataTypeExtensions().get(3).getDecimalType();
        assertEquals(20, decimalType.getPrecision());
        assertEquals(4, decimalType.getScale());
    }

    @Test
    void shouldMapScalarLogicalTypes() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("ts", DataTypes.TIMESTAMP(3).notNull()),
                Column.physical("bool_col", DataTypes.BOOLEAN()),
                Column.physical("tiny_col", DataTypes.TINYINT()),
                Column.physical("small_col", DataTypes.SMALLINT()),
                Column.physical("int_col", DataTypes.INT()),
                Column.physical("big_col", DataTypes.BIGINT()),
                Column.physical("float_col", DataTypes.FLOAT()),
                Column.physical("double_col", DataTypes.DOUBLE()),
                Column.physical("char_col", DataTypes.CHAR(8)),
                Column.physical("varchar_col", DataTypes.VARCHAR(32)),
                Column.physical("binary_col", DataTypes.BINARY(4)),
                Column.physical("varbinary_col", DataTypes.VARBINARY(16)),
                Column.physical("date_col", DataTypes.DATE()),
                Column.physical("decimal_col", DataTypes.DECIMAL(10, 2))));

        TableSchema tableSchema = GreptimeTableSchemaConverter.convert(
                schema,
                options("metrics", "ts", List.of()));

        assertEquals(
                List.of(
                        Common.ColumnDataType.TIMESTAMP_MILLISECOND,
                        Common.ColumnDataType.BOOLEAN,
                        Common.ColumnDataType.INT8,
                        Common.ColumnDataType.INT16,
                        Common.ColumnDataType.INT32,
                        Common.ColumnDataType.INT64,
                        Common.ColumnDataType.FLOAT32,
                        Common.ColumnDataType.FLOAT64,
                        Common.ColumnDataType.STRING,
                        Common.ColumnDataType.STRING,
                        Common.ColumnDataType.BINARY,
                        Common.ColumnDataType.BINARY,
                        Common.ColumnDataType.DATE,
                        Common.ColumnDataType.DECIMAL128),
                tableSchema.getDataTypes());
    }

    @Test
    void shouldMapTimeAndTimestampPrecisionBuckets() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("ts0", DataTypes.TIMESTAMP(0).notNull()),
                Column.physical("ts3", DataTypes.TIMESTAMP(3)),
                Column.physical("ts6", DataTypes.TIMESTAMP(6)),
                Column.physical("ts9", DataTypes.TIMESTAMP(9)),
                Column.physical("ltz0", DataTypes.TIMESTAMP_LTZ(0)),
                Column.physical("ltz3", DataTypes.TIMESTAMP_LTZ(3)),
                Column.physical("ltz6", DataTypes.TIMESTAMP_LTZ(6)),
                Column.physical("ltz9", DataTypes.TIMESTAMP_LTZ(9)),
                Column.physical("time0", DataTypes.TIME(0)),
                Column.physical("time3", DataTypes.TIME(3)),
                Column.physical("time6", DataTypes.TIME(6)),
                Column.physical("time9", DataTypes.TIME(9))));

        TableSchema tableSchema = GreptimeTableSchemaConverter.convert(
                schema,
                options("metrics", "ts0", List.of()));

        assertEquals(
                List.of(
                        Common.ColumnDataType.TIMESTAMP_SECOND,
                        Common.ColumnDataType.TIMESTAMP_MILLISECOND,
                        Common.ColumnDataType.TIMESTAMP_MICROSECOND,
                        Common.ColumnDataType.TIMESTAMP_NANOSECOND,
                        Common.ColumnDataType.TIMESTAMP_SECOND,
                        Common.ColumnDataType.TIMESTAMP_MILLISECOND,
                        Common.ColumnDataType.TIMESTAMP_MICROSECOND,
                        Common.ColumnDataType.TIMESTAMP_NANOSECOND,
                        Common.ColumnDataType.TIME_SECOND,
                        Common.ColumnDataType.TIME_MILLISECOND,
                        Common.ColumnDataType.TIME_MICROSECOND,
                        Common.ColumnDataType.TIME_NANOSECOND),
                tableSchema.getDataTypes());
    }

    @Test
    void shouldRejectUnsupportedLogicalTypes() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("ts", DataTypes.TIMESTAMP(3).notNull()),
                Column.physical("payload", DataTypes.ARRAY(DataTypes.INT()))));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchemaConverter.convert(schema, options("metrics", "ts", List.of())));

        assertTrue(error.getMessage().contains("payload"));
        assertTrue(error.getMessage().contains("ARRAY"));
    }

    @Test
    void shouldRejectNullableTimeIndex() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("host", DataTypes.STRING()),
                Column.physical("ts", DataTypes.TIMESTAMP(3))));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeTableSchemaConverter.convert(schema, options("metrics", "ts", List.of())));

        assertEquals("time-index column must not be nullable: ts", error.getMessage());
    }

    private static ResolvedSchema resolvedSchema(List<Column> columns) {
        return new ResolvedSchema(columns, List.of(), null);
    }

    private static GreptimeTableSinkOptions options(String table, String timeIndex, List<String> tags) {
        return new GreptimeTableSinkOptions(
                List.of("127.0.0.1:4001"),
                timeIndex,
                GreptimeConnectorOptions.DEFAULT_DATABASE,
                table,
                null,
                null,
                tags,
                GreptimeConnectorOptions.DEFAULT_BATCH_MAX_ROWS,
                GreptimeConnectorOptions.DEFAULT_BULK_TIMEOUT_MS_PER_MESSAGE,
                GreptimeConnectorOptions.DEFAULT_BULK_MAX_REQUESTS_IN_FLIGHT,
                GreptimeConnectorOptions.DEFAULT_BULK_ALLOCATOR_INIT_RESERVATION,
                GreptimeConnectorOptions.DEFAULT_BULK_ALLOCATOR_MAX_ALLOCATION);
    }
}
