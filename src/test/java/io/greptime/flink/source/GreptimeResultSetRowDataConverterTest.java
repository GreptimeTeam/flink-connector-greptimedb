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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeResultSetRowDataConverterTest {

    @Test
    void shouldConvertSupportedTypes() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("tiny_value", DataTypes.TINYINT()),
                        DataTypes.FIELD("small_value", DataTypes.SMALLINT()),
                        DataTypes.FIELD("int_value", DataTypes.INT()),
                        DataTypes.FIELD("big_value", DataTypes.BIGINT()),
                        DataTypes.FIELD("float_value", DataTypes.FLOAT()),
                        DataTypes.FIELD("double_value", DataTypes.DOUBLE()),
                        DataTypes.FIELD("char_value", DataTypes.CHAR(4)),
                        DataTypes.FIELD("text_value", DataTypes.STRING()),
                        DataTypes.FIELD("binary_value", DataTypes.BINARY(3)),
                        DataTypes.FIELD("varbinary_value", DataTypes.VARBINARY(3)),
                        DataTypes.FIELD("date_value", DataTypes.DATE()),
                        DataTypes.FIELD("decimal_value", DataTypes.DECIMAL(10, 2)),
                        DataTypes.FIELD("timestamp_value", DataTypes.TIMESTAMP(3)))
                .getLogicalType();
        LocalDate date = LocalDate.of(2026, 7, 10);
        LocalDateTime timestamp = LocalDateTime.of(2026, 7, 10, 12, 34, 56, 123_000_000);

        RowData row = new GreptimeResultSetRowDataConverter(rowType).convert(resultSet(new Object[] {
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                5.5f,
                6.5d,
                "char",
                "text",
                new byte[] {1, 2, 3},
                new byte[] {4, 5, 6},
                Date.valueOf(date),
                new BigDecimal("12.34"),
                Timestamp.valueOf(timestamp)
        }));

        assertEquals(true, row.getBoolean(0));
        assertEquals((byte) 1, row.getByte(1));
        assertEquals((short) 2, row.getShort(2));
        assertEquals(3, row.getInt(3));
        assertEquals(4L, row.getLong(4));
        assertEquals(5.5f, row.getFloat(5));
        assertEquals(6.5d, row.getDouble(6));
        assertEquals(StringData.fromString("char"), row.getString(7));
        assertEquals(StringData.fromString("text"), row.getString(8));
        assertArrayEquals(new byte[] {1, 2, 3}, row.getBinary(9));
        assertArrayEquals(new byte[] {4, 5, 6}, row.getBinary(10));
        assertEquals((int) date.toEpochDay(), row.getInt(11));
        assertEquals(DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2), row.getDecimal(12, 10, 2));
        assertEquals(TimestampData.fromLocalDateTime(timestamp), row.getTimestamp(13, 3));
    }

    @Test
    void shouldPreserveNullValues() throws Exception {
        RowType rowType = (RowType) DataTypes.ROW(
                        DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
                        DataTypes.FIELD("value", DataTypes.INT()),
                        DataTypes.FIELD("text", DataTypes.STRING()))
                .getLogicalType();

        RowData row = new GreptimeResultSetRowDataConverter(rowType)
                .convert(resultSet(new Object[] {null, null, null}));

        assertTrue(row.isNullAt(0));
        assertTrue(row.isNullAt(1));
        assertTrue(row.isNullAt(2));
    }

    @Test
    void shouldRejectDecimalOverflow() {
        RowType rowType = (RowType) DataTypes.ROW(
                        DataTypes.FIELD("amount", DataTypes.DECIMAL(3, 2)))
                .getLogicalType();

        SQLException error = assertThrows(
                SQLException.class,
                () -> new GreptimeResultSetRowDataConverter(rowType)
                        .convert(resultSet(new Object[] {new BigDecimal("123.45")})));
        assertEquals(
                "DECIMAL value exceeds declared precision and scale for column `amount`",
                error.getMessage());
    }

    private static ResultSet resultSet(Object[] values) {
        class State {
            private boolean wasNull;
        }
        State state = new State();
        return (ResultSet) Proxy.newProxyInstance(
                GreptimeResultSetRowDataConverterTest.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (proxy, method, args) -> {
                    if ("wasNull".equals(method.getName())) {
                        return state.wasNull;
                    }
                    if (method.getName().startsWith("get") && args != null && args.length == 1) {
                        Object value = values[(int) args[0] - 1];
                        state.wasNull = value == null;
                        return value == null ? defaultValue(method.getReturnType()) : value;
                    }
                    if ("toString".equals(method.getName())) {
                        return "TestResultSet";
                    }
                    return defaultValue(method.getReturnType());
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0f;
        }
        if (type == double.class) {
            return 0d;
        }
        return null;
    }
}
