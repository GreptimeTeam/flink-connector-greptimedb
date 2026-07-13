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

import io.greptime.flink.table.GreptimeDynamicTableSinkFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeDynamicTableSourceFactoryTest {

    @Test
    void shouldExposeSourceFactoryOptions() {
        GreptimeDynamicTableSourceFactory factory = new GreptimeDynamicTableSourceFactory();

        assertEquals(GreptimeSourceOptions.IDENTIFIER, factory.factoryIdentifier());
        assertEquals(Set.of(GreptimeSourceOptions.JDBC_URL), factory.requiredOptions());

        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.DATABASE));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.TABLE));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.USERNAME));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.PASSWORD));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.CONNECT_TIMEOUT_MS));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.SOCKET_TIMEOUT_MS));
        assertTrue(optionalOptions.contains(GreptimeSourceOptions.FETCH_SIZE));

        Set<ConfigOption<?>> forwardOptions = factory.forwardOptions();
        assertTrue(forwardOptions.contains(GreptimeSourceOptions.JDBC_URL));
        assertTrue(forwardOptions.contains(GreptimeSourceOptions.USERNAME));
        assertTrue(forwardOptions.contains(GreptimeSourceOptions.PASSWORD));
        assertTrue(forwardOptions.contains(GreptimeSourceOptions.CONNECT_TIMEOUT_MS));
        assertTrue(forwardOptions.contains(GreptimeSourceOptions.SOCKET_TIMEOUT_MS));
        assertFalse(forwardOptions.contains(GreptimeSourceOptions.TABLE));
        assertFalse(forwardOptions.contains(GreptimeSourceOptions.FETCH_SIZE));
    }

    @Test
    void shouldDiscoverSourceAndSinkFactoriesByType() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        assertInstanceOf(
                GreptimeDynamicTableSourceFactory.class,
                FactoryUtil.discoverFactory(
                        classLoader,
                        DynamicTableSourceFactory.class,
                        GreptimeSourceOptions.IDENTIFIER));
        assertInstanceOf(
                GreptimeDynamicTableSinkFactory.class,
                FactoryUtil.discoverFactory(
                        classLoader,
                        DynamicTableSinkFactory.class,
                        GreptimeSourceOptions.IDENTIFIER));
    }

    @Test
    void shouldCreateBoundedSourceWithValidatedOptions() throws Exception {
        Map<String, String> options = baseOptions();
        options.put(GreptimeSourceOptions.DATABASE.key(), "metrics_db");
        options.put(GreptimeSourceOptions.TABLE.key(), "source_metrics");
        options.put(GreptimeSourceOptions.USERNAME.key(), "reader");
        options.put(GreptimeSourceOptions.PASSWORD.key(), "secret");
        options.put(GreptimeSourceOptions.CONNECT_TIMEOUT_MS.key(), "1234");
        options.put(GreptimeSourceOptions.SOCKET_TIMEOUT_MS.key(), "5678");
        options.put(GreptimeSourceOptions.FETCH_SIZE.key(), "128");

        GreptimeDynamicTableSource source = createSource(options, resolvedSchema());
        GreptimeJdbcQueryConfig config = source.queryConfig();

        assertEquals("jdbc:mysql://127.0.0.1:4002/public?useSSL=false", config.jdbcUrl());
        assertEquals("metrics_db", config.database());
        assertEquals("source_metrics", config.table());
        assertEquals("reader", config.connectionProperties().getProperty("user"));
        assertEquals("secret", config.connectionProperties().getProperty("password"));
        assertEquals("1234", config.connectionProperties().getProperty("connectTimeout"));
        assertEquals("5678", config.connectionProperties().getProperty("socketTimeout"));
        assertEquals(128, config.fetchSize());
        assertEquals("GreptimeDB Bounded Table Source", source.asSummaryString());
        assertEquals(
                source.getChangelogMode(),
                assertInstanceOf(GreptimeDynamicTableSource.class, source.copy()).getChangelogMode());

        InputFormatProvider provider = assertInstanceOf(
                InputFormatProvider.class,
                source.getScanRuntimeProvider(new TestScanContext()));
        assertTrue(provider.isBounded());
        assertEquals(1, provider.getParallelism().orElseThrow());
        try (ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream())) {
            output.writeObject(provider.createInputFormat());
        }
    }

    @Test
    void shouldUseSourceDefaults() {
        GreptimeDynamicTableSource source = createSource(baseOptions(), resolvedSchema());
        GreptimeJdbcQueryConfig config = source.queryConfig();

        assertEquals(GreptimeSourceOptions.DEFAULT_DATABASE, config.database());
        assertEquals("identifier_metrics", config.table());
        assertEquals(GreptimeSourceOptions.DEFAULT_FETCH_SIZE, config.fetchSize());
        assertEquals(
                Integer.toString(GreptimeSourceOptions.DEFAULT_CONNECT_TIMEOUT_MS),
                config.connectionProperties().getProperty("connectTimeout"));
        assertEquals(
                Integer.toString(GreptimeSourceOptions.DEFAULT_SOCKET_TIMEOUT_MS),
                config.connectionProperties().getProperty("socketTimeout"));
    }

    @Test
    void shouldRejectInvalidSourceOptions() {
        ValidationException missingUrl = assertThrows(
                ValidationException.class,
                () -> createSource(Map.of(), resolvedSchema()));
        assertTrue(missingUrl.getMessage().contains("Missing required options"));

        assertOptionError(GreptimeSourceOptions.JDBC_URL.key(), " ", "`query.jdbc-url` must not be blank");
        assertOptionError(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:postgresql://127.0.0.1:4003/public",
                "GreptimeDB source supports only MySQL JDBC URLs");
        assertOptionError(GreptimeSourceOptions.DATABASE.key(), " ", "`database` must not be blank");
        assertOptionError(GreptimeSourceOptions.TABLE.key(), " metrics ",
                "`table` must not have leading or trailing whitespace");
        assertOptionError(GreptimeSourceOptions.CONNECT_TIMEOUT_MS.key(), "0",
                "`query.connect-timeout-ms` must be greater than 0");
        assertOptionError(GreptimeSourceOptions.SOCKET_TIMEOUT_MS.key(), "0",
                "`query.socket-timeout-ms` must be greater than 0");
        assertOptionError(GreptimeSourceOptions.FETCH_SIZE.key(), "-1",
                "`query.fetch-size` must be greater than or equal to 0");
    }

    @Test
    void shouldRejectInvalidAuthenticationAndJdbcTimeoutContents() {
        Map<String, String> usernameOnly = baseOptions();
        usernameOnly.put(GreptimeSourceOptions.USERNAME.key(), "reader");
        assertEquals(
                "`username` and `password` must be configured together",
                assertThrows(IllegalArgumentException.class, () -> createSource(usernameOnly, resolvedSchema()))
                        .getMessage());

        Map<String, String> passwordOnly = baseOptions();
        passwordOnly.put(GreptimeSourceOptions.PASSWORD.key(), "secret");
        assertEquals(
                "`username` and `password` must be configured together",
                assertThrows(IllegalArgumentException.class, () -> createSource(passwordOnly, resolvedSchema()))
                        .getMessage());

        assertOptionError(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:mysql://127.0.0.1:4002/public?connectTimeout=1",
                "`query.jdbc-url` must not configure JDBC timeouts; use the typed timeout options instead");
        assertOptionError(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:mysql://address=(host=127.0.0.1)(port=4002)(socketTimeout=1)/public",
                "`query.jdbc-url` must not configure JDBC timeouts; use the typed timeout options instead");
    }

    @Test
    void shouldDeferSensitiveAndMalformedJdbcUrlValidation() {
        String sensitiveMessage = "`query.jdbc-url` must not contain credentials or authentication tokens; "
                + "use `username` and `password` options instead";
        assertDeferredUrlError(
                "jdbc:mysql://reader:secret@127.0.0.1:4002/public",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://reader:secret%40127.0.0.1:4002/public",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://(host=127.0.0.1,port=4002,password=secret)/public",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://address=(host=127.0.0.1)(port=4002)"
                        + "(trustCertificateKeyStorePassword=secret)/public",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/public?password=secret",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/public?clientCertificateKeyStorePassword=secret",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/public?trustCertificateKeyStorePassword=secret",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/public?%70assword=secret",
                sensitiveMessage);
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/public?pa%zzsword=secret",
                "Invalid percent-encoding in `query.jdbc-url`");
        assertDeferredUrlError(
                "jdbc:mysql://127.0.0.1:4002/publ%zzic",
                "Invalid percent-encoding in `query.jdbc-url`");

        Map<String, String> invalidTypedOption = baseOptions();
        invalidTypedOption.put(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:mysql://127.0.0.1:4002/public?password=secret");
        invalidTypedOption.put(GreptimeSourceOptions.FETCH_SIZE.key(), "invalid");
        assertDeferredFailure(createSource(invalidTypedOption, resolvedSchema()), sensitiveMessage);

        String safeUrl = "jdbc:mysql://127.0.0.1:4002/public?useSSL=false";
        Map<String, String> sensitiveCatalog = Map.of(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:mysql://127.0.0.1:4002/public?password=secret");
        assertDeferredFailure(
                createSource(sensitiveCatalog, Map.of(GreptimeSourceOptions.JDBC_URL.key(), safeUrl)),
                sensitiveMessage);
        assertDeferredFailure(
                createSource(
                        Map.of(GreptimeSourceOptions.JDBC_URL.key(), safeUrl),
                        Map.of(
                                GreptimeSourceOptions.JDBC_URL.key(),
                                "jdbc:mysql://127.0.0.1:4002/public?password=secret")),
                sensitiveMessage);
    }

    @Test
    void shouldRejectUnsupportedSourceTypes() {
        IllegalArgumentException timestampLtz = assertThrows(
                IllegalArgumentException.class,
                () -> createSource(baseOptions(), resolvedSchema(Column.physical("ts", DataTypes.TIMESTAMP_LTZ(3)))));
        assertTrue(timestampLtz.getMessage().contains("TIMESTAMP_LTZ(3)"));

        IllegalArgumentException time = assertThrows(
                IllegalArgumentException.class,
                () -> createSource(baseOptions(), resolvedSchema(Column.physical("time_value", DataTypes.TIME(3)))));
        assertTrue(time.getMessage().contains("TIME(3)"));
    }

    private static void assertOptionError(String key, String value, String expectedMessage) {
        Map<String, String> options = baseOptions();
        options.put(key, value);
        assertEquals(
                expectedMessage,
                assertThrows(IllegalArgumentException.class, () -> createSource(options, resolvedSchema()))
                        .getMessage());
    }

    private static void assertDeferredUrlError(String jdbcUrl, String expectedMessage) {
        Map<String, String> options = baseOptions();
        options.put(GreptimeSourceOptions.JDBC_URL.key(), jdbcUrl);
        assertDeferredFailure(createSource(options, resolvedSchema()), expectedMessage);
    }

    private static void assertDeferredFailure(
            GreptimeDynamicTableSource source,
            String expectedMessage) {
        assertNull(source.queryConfig());
        assertEquals(
                expectedMessage,
                assertThrows(
                                ValidationException.class,
                                () -> source.getScanRuntimeProvider(new TestScanContext()))
                        .getMessage());
        GreptimeDynamicTableSource copy =
                assertInstanceOf(GreptimeDynamicTableSource.class, source.copy());
        assertNull(copy.queryConfig());
    }

    private static GreptimeDynamicTableSource createSource(
            Map<String, String> options,
            ResolvedSchema schema) {
        return createSource(options, Map.of(), schema);
    }

    private static GreptimeDynamicTableSource createSource(
            Map<String, String> options,
            Map<String, String> enrichmentOptions) {
        return createSource(options, enrichmentOptions, resolvedSchema());
    }

    private static GreptimeDynamicTableSource createSource(
            Map<String, String> options,
            Map<String, String> enrichmentOptions,
            ResolvedSchema schema) {
        DynamicTableSource source = new GreptimeDynamicTableSourceFactory()
                .createDynamicTableSource(new TestFactoryContext(options, enrichmentOptions, schema));
        return assertInstanceOf(GreptimeDynamicTableSource.class, source);
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(
                GreptimeSourceOptions.JDBC_URL.key(),
                "jdbc:mysql://127.0.0.1:4002/public?useSSL=false");
        return options;
    }

    private static ResolvedSchema resolvedSchema(Column... additionalColumns) {
        List<Column> columns = new java.util.ArrayList<>();
        columns.add(Column.physical("host", DataTypes.STRING()));
        columns.add(Column.physical("usage", DataTypes.DOUBLE()));
        columns.addAll(List.of(additionalColumns));
        return new ResolvedSchema(columns, List.of(), null);
    }

    private static ResolvedSchema resolvedSchema() {
        return resolvedSchema(Column.physical("ts", DataTypes.TIMESTAMP(3)));
    }

    private static final class TestScanContext implements ScanTableSource.ScanContext {

        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
            return (TypeInformation<T>) TypeInformation.of(RowData.class);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
            return (TypeInformation<T>) TypeInformation.of(RowData.class);
        }

        @Override
        public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
            return new DynamicTableSource.DataStructureConverter() {
                @Override
                public void open(RuntimeConverter.Context context) {
                }

                @Override
                public Object toInternal(Object externalStructure) {
                    return externalStructure;
                }
            };
        }
    }

    private static final class TestFactoryContext implements DynamicTableFactory.Context {

        private final ObjectIdentifier objectIdentifier;
        private final ResolvedCatalogTable catalogTable;
        private final Map<String, String> enrichmentOptions;

        private TestFactoryContext(Map<String, String> options, ResolvedSchema resolvedSchema) {
            this(options, Map.of(), resolvedSchema);
        }

        private TestFactoryContext(
                Map<String, String> options,
                Map<String, String> enrichmentOptions,
                ResolvedSchema resolvedSchema) {
            CatalogTable table = CatalogTable.newBuilder()
                    .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                    .options(options)
                    .build();
            this.objectIdentifier = ObjectIdentifier.of(
                    "default_catalog",
                    "default_database",
                    "identifier_metrics");
            this.catalogTable = new ResolvedCatalogTable(table, resolvedSchema);
            this.enrichmentOptions = Map.copyOf(enrichmentOptions);
        }

        @Override
        public ObjectIdentifier getObjectIdentifier() {
            return objectIdentifier;
        }

        @Override
        public ResolvedCatalogTable getCatalogTable() {
            return catalogTable;
        }

        @Override
        public Map<String, String> getEnrichmentOptions() {
            return enrichmentOptions;
        }

        @Override
        public ReadableConfig getConfiguration() {
            return new Configuration();
        }

        @Override
        public ClassLoader getClassLoader() {
            return Thread.currentThread().getContextClassLoader();
        }

        @Override
        public boolean isTemporary() {
            return false;
        }
    }
}
