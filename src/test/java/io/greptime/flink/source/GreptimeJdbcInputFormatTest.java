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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeJdbcInputFormatTest {

    @Test
    void shouldReadRowsAndCloseResourcesAtEof() throws Exception {
        JdbcScenario scenario = JdbcScenario.rows(List.of("host-1"));
        GreptimeJdbcInputFormat inputFormat = inputFormat(scenario);

        inputFormat.open(new GenericInputSplit(0, 1));
        RowData row = inputFormat.nextRecord(null);

        assertEquals(StringData.fromString("host-1"), row.getString(0));
        assertTrue(inputFormat.reachedEnd());
        assertNull(inputFormat.nextRecord(null));
        assertEquals(1, scenario.resultSetCloseCount.get());
        assertEquals(1, scenario.statementCloseCount.get());
        assertEquals(1, scenario.connectionCloseCount.get());
        assertEquals(32, scenario.fetchSize);

        inputFormat.close();
        assertEquals(1, scenario.resultSetCloseCount.get());
        assertEquals(1, scenario.statementCloseCount.get());
        assertEquals(1, scenario.connectionCloseCount.get());
    }

    @Test
    void shouldCloseResourcesWhenExecuteQueryFails() {
        SQLException failure = new SQLException("execute failed", "HY000", 1);
        JdbcScenario scenario = JdbcScenario.executeFailure(failure);

        IOException error = assertThrows(
                IOException.class,
                () -> inputFormat(scenario).open(new GenericInputSplit(0, 1)));

        assertSame(failure, error.getCause());
        assertEquals(0, scenario.resultSetCloseCount.get());
        assertEquals(1, scenario.statementCloseCount.get());
        assertEquals(1, scenario.connectionCloseCount.get());
    }

    @Test
    void shouldCloseResourcesWhenInitialNextFails() {
        SQLException failure = new SQLException("first next failed", "HY000", 2);
        JdbcScenario scenario = JdbcScenario.nextFailure(List.of(), 1, failure);

        IOException error = assertThrows(
                IOException.class,
                () -> inputFormat(scenario).open(new GenericInputSplit(0, 1)));

        assertSame(failure, error.getCause());
        assertTrue(error.getMessage().contains("read next row"));
        assertClosed(scenario);
    }

    @Test
    void shouldCloseResourcesWhenMidScanNextFails() throws Exception {
        SQLException failure = new SQLException("mid next failed", "HY000", 3);
        JdbcScenario scenario = JdbcScenario.nextFailure(List.of("host-1"), 2, failure);
        GreptimeJdbcInputFormat inputFormat = inputFormat(scenario);
        inputFormat.open(new GenericInputSplit(0, 1));

        IOException error = assertThrows(IOException.class, () -> inputFormat.nextRecord(null));

        assertSame(failure, error.getCause());
        assertTrue(error.getMessage().contains("read next row"));
        assertTrue(inputFormat.reachedEnd());
        assertClosed(scenario);
    }

    @Test
    void shouldReportMissingMysqlDriverClearly() {
        GreptimeJdbcInputFormat inputFormat = new GreptimeJdbcInputFormat(
                queryConfig(),
                rowType(),
                List.of("host"),
                TypeInformation.of(RowData.class));
        Thread thread = Thread.currentThread();
        ClassLoader original = thread.getContextClassLoader();
        thread.setContextClassLoader(new ClassLoader(null) {
        });
        try {
            IOException error = assertThrows(
                    IOException.class,
                    () -> inputFormat.open(new GenericInputSplit(0, 1)));
            assertEquals(
                    "MySQL JDBC driver is required for the GreptimeDB table source",
                    error.getMessage());
            assertTrue(error.getCause().getMessage().contains("driver class is not available"));
            assertFalse(error.getMessage().contains(queryConfig().jdbcUrl()));
        } finally {
            thread.setContextClassLoader(original);
        }
    }

    private static void assertClosed(JdbcScenario scenario) {
        assertEquals(1, scenario.resultSetCloseCount.get());
        assertEquals(1, scenario.statementCloseCount.get());
        assertEquals(1, scenario.connectionCloseCount.get());
    }

    private static GreptimeJdbcInputFormat inputFormat(JdbcScenario scenario) {
        return new GreptimeJdbcInputFormat(
                queryConfig(),
                rowType(),
                List.of("host"),
                TypeInformation.of(RowData.class),
                config -> scenario.connection());
    }

    private static GreptimeJdbcQueryConfig queryConfig() {
        return new GreptimeJdbcQueryConfig(
                "jdbc:mysql://127.0.0.1:4002/public?useSSL=false",
                "public",
                "metrics",
                null,
                null,
                10_000,
                300_000,
                32);
    }

    private static RowType rowType() {
        return (RowType) DataTypes.ROW(DataTypes.FIELD("host", DataTypes.STRING())).getLogicalType();
    }

    private static final class JdbcScenario {

        private final List<String> rows;
        private final int failAtNextCall;
        private final SQLException nextFailure;
        private final SQLException executeFailure;
        private final AtomicInteger resultSetCloseCount = new AtomicInteger();
        private final AtomicInteger statementCloseCount = new AtomicInteger();
        private final AtomicInteger connectionCloseCount = new AtomicInteger();
        private int nextCalls;
        private int currentRow = -1;
        private int fetchSize;

        private JdbcScenario(
                List<String> rows,
                int failAtNextCall,
                SQLException nextFailure,
                SQLException executeFailure) {
            this.rows = rows;
            this.failAtNextCall = failAtNextCall;
            this.nextFailure = nextFailure;
            this.executeFailure = executeFailure;
        }

        private static JdbcScenario rows(List<String> rows) {
            return new JdbcScenario(rows, -1, null, null);
        }

        private static JdbcScenario executeFailure(SQLException failure) {
            return new JdbcScenario(List.of(), -1, null, failure);
        }

        private static JdbcScenario nextFailure(List<String> rows, int failAtNextCall, SQLException failure) {
            return new JdbcScenario(rows, failAtNextCall, failure, null);
        }

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "createStatement":
                                return statement();
                            case "close":
                                connectionCloseCount.incrementAndGet();
                                return null;
                            case "isClosed":
                                return connectionCloseCount.get() > 0;
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
        }

        private Statement statement() {
            return (Statement) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Statement.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "setFetchSize":
                                fetchSize = (int) args[0];
                                return null;
                            case "executeQuery":
                                if (executeFailure != null) {
                                    throw executeFailure;
                                }
                                return resultSet();
                            case "close":
                                statementCloseCount.incrementAndGet();
                                return null;
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
        }

        private ResultSet resultSet() {
            return (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {ResultSet.class},
                    (proxy, method, args) -> {
                        switch (method.getName()) {
                            case "next":
                                nextCalls++;
                                if (nextCalls == failAtNextCall) {
                                    throw nextFailure;
                                }
                                currentRow++;
                                return currentRow < rows.size();
                            case "getString":
                                return rows.get(currentRow);
                            case "wasNull":
                                return false;
                            case "close":
                                resultSetCloseCount.incrementAndGet();
                                return null;
                            default:
                                return defaultValue(method.getReturnType());
                        }
                    });
        }

        private static Object defaultValue(Class<?> type) {
            if (!type.isPrimitive()) {
                return null;
            }
            if (type == boolean.class) {
                return false;
            }
            if (type == int.class) {
                return 0;
            }
            if (type == long.class) {
                return 0L;
            }
            return null;
        }
    }
}
