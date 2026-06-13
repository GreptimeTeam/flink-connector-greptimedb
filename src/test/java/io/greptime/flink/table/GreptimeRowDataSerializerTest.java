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

import io.greptime.models.ArrowHelper;
import io.greptime.models.Table;
import io.greptime.models.TableSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeRowDataSerializerTest {

    @Test
    void shouldSerializeSupportedScalarValues() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("bool_col", DataTypes.BOOLEAN()),
                Column.physical("tiny_col", DataTypes.TINYINT()),
                Column.physical("small_col", DataTypes.SMALLINT()),
                Column.physical("int_col", DataTypes.INT()),
                Column.physical("big_col", DataTypes.BIGINT()),
                Column.physical("float_col", DataTypes.FLOAT()),
                Column.physical("double_col", DataTypes.DOUBLE()),
                Column.physical("text_col", DataTypes.STRING()),
                Column.physical("binary_col", DataTypes.BYTES()),
                Column.physical("date_col", DataTypes.DATE()),
                Column.physical("decimal_col", DataTypes.DECIMAL(10, 2)),
                Column.physical("time0_col", DataTypes.TIME(0)),
                Column.physical("time3_col", DataTypes.TIME(3)),
                Column.physical("time6_col", DataTypes.TIME(6)),
                Column.physical("time9_col", DataTypes.TIME(9)),
                Column.physical("ts0_col", DataTypes.TIMESTAMP(0)),
                Column.physical("ts3_col", DataTypes.TIMESTAMP(3)),
                Column.physical("ts6_col", DataTypes.TIMESTAMP(6)),
                Column.physical("ts9_col", DataTypes.TIMESTAMP(9)),
                Column.physical("ltz6_col", DataTypes.TIMESTAMP_LTZ(6))));
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(schema);

        byte[] binary = new byte[] {1, 2, 3};
        LocalDateTime localTimestamp = LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_789);
        Instant localTimestampAsUtc = localTimestamp.toInstant(ZoneOffset.UTC);
        Instant instant = Instant.parse("2024-01-02T03:04:05.987654321Z");
        GenericRowData row = GenericRowData.of(
                true,
                (byte) 7,
                (short) 8,
                9,
                10L,
                1.5F,
                2.5D,
                StringData.fromString("hello"),
                binary,
                19_724,
                DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                12_345,
                12_345,
                12_345,
                12_345,
                TimestampData.fromLocalDateTime(localTimestamp),
                TimestampData.fromLocalDateTime(localTimestamp),
                TimestampData.fromLocalDateTime(localTimestamp),
                TimestampData.fromLocalDateTime(localTimestamp),
                TimestampData.fromInstant(instant));

        Object[] values = serializer.serialize(row);

        assertEquals(true, values[0]);
        assertEquals(7, values[1]);
        assertEquals(8, values[2]);
        assertEquals(9, values[3]);
        assertEquals(10L, values[4]);
        assertEquals(1.5F, values[5]);
        assertEquals(2.5D, values[6]);
        assertEquals("hello", values[7]);
        assertArrayEquals(binary, (byte[]) values[8]);
        assertEquals(19_724, values[9]);
        assertEquals(new BigDecimal("123.45"), values[10]);
        assertEquals(12, values[11]);
        assertEquals(12_345, values[12]);
        assertEquals(12_345_000L, values[13]);
        assertEquals(12_345_000_000L, values[14]);
        assertEquals(localTimestampAsUtc.getEpochSecond(), values[15]);
        assertEquals(localTimestampAsUtc.toEpochMilli(), values[16]);
        assertEquals(toEpochMicros(localTimestampAsUtc), values[17]);
        assertEquals(toEpochNanos(localTimestampAsUtc), values[18]);
        assertEquals(toEpochMicros(instant), values[19]);
    }

    @Test
    void shouldWriteSerializedValuesIntoIngesterTableBuffer() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("tiny_col", DataTypes.TINYINT()),
                Column.physical("small_col", DataTypes.SMALLINT()),
                Column.physical("date_col", DataTypes.DATE()),
                Column.physical("decimal_col", DataTypes.DECIMAL(10, 2)),
                Column.physical("time_col", DataTypes.TIME(6)),
                Column.physical("ts_col", DataTypes.TIMESTAMP(9).notNull())));
        TableSchema tableSchema = GreptimeTableSchemaConverter.convert(
                schema,
                options("metrics", "ts_col", List.of()));
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(schema);
        GenericRowData row = GenericRowData.of(
                (byte) 7,
                (short) 8,
                19_724,
                DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2),
                12_345,
                TimestampData.fromLocalDateTime(LocalDateTime.of(2024, 1, 2, 3, 4, 5, 123_456_789)));

        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(ArrowHelper.createSchema(tableSchema), allocator)) {
            Table.TableBufferRoot buffer = Table.tableBufferRoot(tableSchema, root, 1);

            buffer.addRow(serializer.serialize(row));
            buffer.complete();

            assertEquals(1, buffer.rowCount());
            assertEquals(6, buffer.columnCount());
        }
    }

    @Test
    void shouldPreserveNullValues() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("text_col", DataTypes.STRING()),
                Column.physical("decimal_col", DataTypes.DECIMAL(10, 2)),
                Column.physical("ts_col", DataTypes.TIMESTAMP(3))));
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(schema);

        Object[] values = serializer.serialize(GenericRowData.of(null, null, null));

        assertNull(values[0]);
        assertNull(values[1]);
        assertNull(values[2]);
    }

    @Test
    void shouldRejectNonInsertRows() {
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(resolvedSchema(List.of(
                Column.physical("value", DataTypes.INT()))));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> serializer.serialize(GenericRowData.ofKind(RowKind.UPDATE_AFTER, 1)));

        assertEquals("GreptimeDB Table sink only supports INSERT rows", error.getMessage());
    }

    @Test
    void shouldRejectMismatchedArity() {
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(resolvedSchema(List.of(
                Column.physical("first", DataTypes.INT()),
                Column.physical("second", DataTypes.INT()))));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> serializer.serialize(GenericRowData.of(1)));

        assertEquals("Expected RowData arity: 2, actual: 1", error.getMessage());
    }

    @Test
    void shouldRejectUnsupportedLogicalTypes() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("payload", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> GreptimeRowDataSerializer.create(schema));

        assertTrue(error.getMessage().contains("payload"));
        assertTrue(error.getMessage().contains("MAP"));
    }

    @Test
    void shouldWrapTimestampOverflowWithColumnContext() {
        ResolvedSchema schema = resolvedSchema(List.of(
                Column.physical("ts", DataTypes.TIMESTAMP(9))));
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(schema);
        GenericRowData row = GenericRowData.of(TimestampData.fromInstant(Instant.MAX));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> serializer.serialize(row));

        assertEquals(
                "Timestamp value out of range for GreptimeDB column `ts` with precision 9",
                error.getMessage());
        assertTrue(error.getCause() instanceof ArithmeticException);
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
                GreptimeConnectorOptions.DEFAULT_BATCH_MAX_ROWS);
    }

    private static long toEpochMicros(Instant instant) {
        return Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                instant.getNano() / 1_000L);
    }

    private static long toEpochNanos(Instant instant) {
        return Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L),
                instant.getNano());
    }
}
