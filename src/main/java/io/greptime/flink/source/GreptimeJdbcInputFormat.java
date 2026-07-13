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

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

final class GreptimeJdbcInputFormat extends RichInputFormat<RowData, GenericInputSplit>
        implements NonParallelInput, ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private final GreptimeJdbcQueryConfig queryConfig;
    private final TypeInformation<RowData> producedType;
    private final String sql;
    private final GreptimeResultSetRowDataConverter converter;
    private final JdbcConnectionFactory connectionFactory;

    private transient Connection connection;
    private transient Statement statement;
    private transient ResultSet resultSet;
    private transient boolean hasNext;

    GreptimeJdbcInputFormat(
            GreptimeJdbcQueryConfig queryConfig,
            RowType rowType,
            List<String> columns,
            TypeInformation<RowData> producedType) {
        this(queryConfig, rowType, columns, producedType, DriverManagerConnectionFactory.INSTANCE);
    }

    GreptimeJdbcInputFormat(
            GreptimeJdbcQueryConfig queryConfig,
            RowType rowType,
            List<String> columns,
            TypeInformation<RowData> producedType,
            JdbcConnectionFactory connectionFactory) {
        this.queryConfig = Objects.requireNonNull(queryConfig, "queryConfig must not be null");
        this.producedType = Objects.requireNonNull(producedType, "producedType must not be null");
        this.sql = GreptimeQuerySqlBuilder.buildSelect(queryConfig.database(), queryConfig.table(), columns);
        this.converter = new GreptimeResultSetRowDataConverter(rowType);
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory must not be null");
    }

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return cachedStatistics;
    }

    @Override
    public GenericInputSplit[] createInputSplits(int minNumSplits) {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        if (split.getSplitNumber() != 0 || split.getTotalNumberOfSplits() != 1) {
            throw new IOException("GreptimeDB source expects exactly one input split");
        }
        close();

        try {
            connection = connectionFactory.connect(queryConfig);
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (queryConfig.fetchSize() > 0) {
                statement.setFetchSize(queryConfig.fetchSize());
            }
            resultSet = statement.executeQuery(sql);
        } catch (SQLException e) {
            throw queryFailure("execute query", e);
        }
        advance();
    }

    @Override
    public boolean reachedEnd() {
        return !hasNext;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        if (!hasNext) {
            return null;
        }

        RowData row;
        try {
            row = converter.convert(resultSet);
        } catch (SQLException e) {
            throw queryFailure("convert row", e);
        }
        advance();
        return row;
    }

    @Override
    public void close() throws IOException {
        hasNext = false;
        ResultSet resultSet = this.resultSet;
        Statement statement = this.statement;
        Connection connection = this.connection;
        this.resultSet = null;
        this.statement = null;
        this.connection = null;

        SQLException failure = null;
        failure = closeResource(resultSet, failure);
        failure = closeResource(statement, failure);
        failure = closeResource(connection, failure);
        if (failure != null) {
            throw new IOException("Failed to close GreptimeDB JDBC source resources", failure);
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    String sql() {
        return sql;
    }

    private void advance() throws IOException {
        try {
            hasNext = resultSet.next();
        } catch (SQLException e) {
            throw queryFailure("read next row", e);
        }
        if (!hasNext) {
            close();
        }
    }

    private IOException queryFailure(String operation, SQLException cause) {
        String message;
        if (cause instanceof MissingJdbcDriverException) {
            message = "MySQL JDBC driver is required for the GreptimeDB table source";
        } else {
            message = "GreptimeDB JDBC source failed to "
                    + operation
                    + " for table `"
                    + queryConfig.database()
                    + "`.`"
                    + queryConfig.table()
                    + "`";
        }
        IOException failure = new IOException(message, cause);
        try {
            close();
        } catch (IOException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    private static SQLException closeResource(AutoCloseable resource, SQLException firstFailure) {
        if (resource == null) {
            return firstFailure;
        }
        try {
            resource.close();
            return firstFailure;
        } catch (Exception e) {
            SQLException failure = e instanceof SQLException
                    ? (SQLException) e
                    : new SQLException("Failed to close JDBC resource", e);
            if (firstFailure == null) {
                return failure;
            }
            firstFailure.addSuppressed(failure);
            return firstFailure;
        }
    }

    @FunctionalInterface
    interface JdbcConnectionFactory extends Serializable {
        Connection connect(GreptimeJdbcQueryConfig config) throws SQLException;
    }

    private enum DriverManagerConnectionFactory implements JdbcConnectionFactory {
        INSTANCE;

        @Override
        public Connection connect(GreptimeJdbcQueryConfig config) throws SQLException {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = GreptimeJdbcInputFormat.class.getClassLoader();
            }
            try {
                Class.forName(MYSQL_DRIVER_CLASS, true, classLoader);
            } catch (ClassNotFoundException e) {
                throw new MissingJdbcDriverException(e);
            }
            return DriverManager.getConnection(config.jdbcUrl(), config.connectionProperties());
        }
    }

    private static final class MissingJdbcDriverException extends SQLException {

        private static final long serialVersionUID = 1L;

        private MissingJdbcDriverException(ClassNotFoundException cause) {
            super("MySQL JDBC driver class is not available", cause);
        }
    }
}
