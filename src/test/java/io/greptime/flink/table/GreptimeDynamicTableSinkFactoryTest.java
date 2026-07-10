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

import io.greptime.flink.sink.GreptimeSink;
import org.apache.flink.api.connector.sink2.Sink;
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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeDynamicTableSinkFactoryTest {

    @Test
    void shouldExposeSinkFactoryOptions() {
        GreptimeDynamicTableSinkFactory factory = new GreptimeDynamicTableSinkFactory();

        assertEquals(GreptimeConnectorOptions.IDENTIFIER, factory.factoryIdentifier());
        assertEquals(Set.of(GreptimeConnectorOptions.ENDPOINTS, GreptimeConnectorOptions.TIME_INDEX),
                factory.requiredOptions());

        Set<ConfigOption<?>> optionalOptions = factory.optionalOptions();
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.DATABASE));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.TABLE));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.USERNAME));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.PASSWORD));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.TAGS));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.BATCH_MAX_ROWS));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION));
        assertTrue(optionalOptions.contains(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION));

        Set<ConfigOption<?>> forwardOptions = factory.forwardOptions();
        assertTrue(forwardOptions.contains(GreptimeConnectorOptions.ENDPOINTS));
        assertTrue(forwardOptions.contains(GreptimeConnectorOptions.USERNAME));
        assertTrue(forwardOptions.contains(GreptimeConnectorOptions.PASSWORD));
        assertFalse(forwardOptions.contains(GreptimeConnectorOptions.TABLE));
        assertFalse(forwardOptions.contains(GreptimeConnectorOptions.TIME_INDEX));
        assertFalse(forwardOptions.contains(GreptimeConnectorOptions.TAGS));
    }

    @Test
    void shouldCreateSinkWithValidatedOptions() {
        Map<String, String> options = baseOptions();
        options.put(GreptimeConnectorOptions.DATABASE.key(), "metrics_db");
        options.put(GreptimeConnectorOptions.TABLE.key(), "factory_metrics");
        options.put(GreptimeConnectorOptions.USERNAME.key(), "greptime");
        options.put(GreptimeConnectorOptions.PASSWORD.key(), "secret");
        options.put(GreptimeConnectorOptions.TAGS.key(), "host,region");
        options.put(GreptimeConnectorOptions.BATCH_MAX_ROWS.key(), "128");
        options.put(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE.key(), "30000");
        options.put(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT.key(), "4");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION.key(), "1024");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION.key(), "2048");

        GreptimeDynamicTableSink sink = createSink(options, "identifier_metrics");
        GreptimeTableSinkOptions sinkOptions = sink.options();

        assertEquals(List.of("127.0.0.1:4001"), sinkOptions.endpoints());
        assertEquals("ts", sinkOptions.timeIndex());
        assertEquals("metrics_db", sinkOptions.database());
        assertEquals("factory_metrics", sinkOptions.table());
        assertEquals("greptime", sinkOptions.username());
        assertEquals("secret", sinkOptions.password());
        assertEquals(List.of("host", "region"), sinkOptions.tags());
        assertEquals(128, sinkOptions.batchMaxRows());
        assertEquals(30_000L, sinkOptions.bulkTimeoutMsPerMessage());
        assertEquals(4, sinkOptions.bulkMaxRequestsInFlight());
        assertEquals(1_024L, sinkOptions.bulkAllocatorInitReservation());
        assertEquals(2_048L, sinkOptions.bulkAllocatorMaxAllocation());
        assertEquals(ChangelogMode.insertOnly(), sink.getChangelogMode(ChangelogMode.all()));
        assertTrue(sink.getChangelogMode(ChangelogMode.all()).containsOnly(RowKind.INSERT));
    }

    @Test
    void shouldUseIdentifierTableWhenTableOptionIsMissing() {
        GreptimeDynamicTableSink sink = createSink(baseOptions(), "identifier_metrics");

        assertEquals(GreptimeConnectorOptions.DEFAULT_DATABASE, sink.options().database());
        assertEquals("identifier_metrics", sink.options().table());
        assertEquals(List.of(), sink.options().tags());
        assertEquals(GreptimeConnectorOptions.DEFAULT_BATCH_MAX_ROWS, sink.options().batchMaxRows());
        assertEquals(
                GreptimeConnectorOptions.DEFAULT_BULK_TIMEOUT_MS_PER_MESSAGE,
                sink.options().bulkTimeoutMsPerMessage());
        assertEquals(
                GreptimeConnectorOptions.DEFAULT_BULK_MAX_REQUESTS_IN_FLIGHT,
                sink.options().bulkMaxRequestsInFlight());
        assertEquals(
                GreptimeConnectorOptions.DEFAULT_BULK_ALLOCATOR_INIT_RESERVATION,
                sink.options().bulkAllocatorInitReservation());
        assertEquals(
                GreptimeConnectorOptions.DEFAULT_BULK_ALLOCATOR_MAX_ALLOCATION,
                sink.options().bulkAllocatorMaxAllocation());
    }

    @Test
    void shouldRejectMissingRequiredOptions() {
        Map<String, String> noEndpoints = new HashMap<>();
        noEndpoints.put(GreptimeConnectorOptions.TIME_INDEX.key(), "ts");

        ValidationException missingEndpoints = assertThrows(
                ValidationException.class,
                () -> createSink(noEndpoints, "metrics"));
        assertTrue(missingEndpoints.getMessage().contains("Missing required options"));
        assertTrue(missingEndpoints.getMessage().contains(GreptimeConnectorOptions.ENDPOINTS.key()));

        Map<String, String> noTimeIndex = new HashMap<>();
        noTimeIndex.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001");

        ValidationException missingTimeIndex = assertThrows(
                ValidationException.class,
                () -> createSink(noTimeIndex, "metrics"));
        assertTrue(missingTimeIndex.getMessage().contains("Missing required options"));
        assertTrue(missingTimeIndex.getMessage().contains(GreptimeConnectorOptions.TIME_INDEX.key()));
    }

    @Test
    void shouldRejectUnknownOptions() {
        Map<String, String> options = baseOptions();
        options.put("batch.max-row", "100");

        ValidationException error = assertThrows(
                ValidationException.class,
                () -> createSink(options, "metrics"));

        assertTrue(error.getMessage().contains("Unsupported options"));
        assertTrue(error.getMessage().contains("batch.max-row"));
    }

    @Test
    void shouldRejectInvalidTextOptions() {
        assertOptionError(GreptimeConnectorOptions.ENDPOINTS.key(), " ", "`endpoints` must not be blank");
        assertOptionError(GreptimeConnectorOptions.TIME_INDEX.key(), "", "`time-index` must not be blank");
        assertOptionError(
                GreptimeConnectorOptions.TIME_INDEX.key(),
                " ts ",
                "`time-index` must not have leading or trailing whitespace");
        assertOptionError(GreptimeConnectorOptions.DATABASE.key(), " ", "`database` must not be blank");
        assertOptionError(GreptimeConnectorOptions.TABLE.key(), "", "`table` must not be blank");
        assertOptionError(
                GreptimeConnectorOptions.DATABASE.key(),
                " public ",
                "`database` must not have leading or trailing whitespace");
        assertOptionError(
                GreptimeConnectorOptions.TABLE.key(),
                " metrics ",
                "`table` must not have leading or trailing whitespace");
    }

    @Test
    void shouldRejectIncompleteAuthPair() {
        Map<String, String> usernameOnly = baseOptions();
        usernameOnly.put(GreptimeConnectorOptions.USERNAME.key(), "greptime");

        IllegalArgumentException usernameError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(usernameOnly, "metrics"));
        assertEquals("`username` and `password` must be configured together", usernameError.getMessage());

        Map<String, String> passwordOnly = baseOptions();
        passwordOnly.put(GreptimeConnectorOptions.PASSWORD.key(), "secret");

        IllegalArgumentException passwordError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(passwordOnly, "metrics"));
        assertEquals("`username` and `password` must be configured together", passwordError.getMessage());
    }

    @Test
    void shouldRejectBlankAuthValues() {
        Map<String, String> blankUsername = baseOptions();
        blankUsername.put(GreptimeConnectorOptions.USERNAME.key(), " ");
        blankUsername.put(GreptimeConnectorOptions.PASSWORD.key(), "secret");

        assertEquals(
                "`username` must not be blank",
                assertThrows(IllegalArgumentException.class, () -> createSink(blankUsername, "metrics")).getMessage());

        Map<String, String> blankPassword = baseOptions();
        blankPassword.put(GreptimeConnectorOptions.USERNAME.key(), "greptime");
        blankPassword.put(GreptimeConnectorOptions.PASSWORD.key(), "");

        assertEquals(
                "`password` must not be blank",
                assertThrows(IllegalArgumentException.class, () -> createSink(blankPassword, "metrics")).getMessage());
    }

    @Test
    void shouldRejectInvalidTags() {
        Map<String, String> blankTag = baseOptions();
        blankTag.put(GreptimeConnectorOptions.TAGS.key(), "host, ");

        IllegalArgumentException blankError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(blankTag, "metrics"));
        assertEquals("`tags` must not be blank", blankError.getMessage());

        Map<String, String> duplicateTag = baseOptions();
        duplicateTag.put(GreptimeConnectorOptions.TAGS.key(), "host,region,host");

        IllegalArgumentException duplicateError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(duplicateTag, "metrics"));
        assertEquals("Duplicate tag column: host", duplicateError.getMessage());
    }

    @Test
    void shouldRejectInvalidBatchMaxRows() {
        Map<String, String> options = baseOptions();
        options.put(GreptimeConnectorOptions.BATCH_MAX_ROWS.key(), "0");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(options, "metrics"));
        assertEquals("`batch.max-rows` must be greater than 0", error.getMessage());
    }

    @Test
    void shouldRejectInvalidBulkWriteOptions() {
        assertOptionError(
                GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE.key(),
                "0",
                "`bulk.timeout-ms-per-message` must be greater than 0");
        assertOptionError(
                GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT.key(),
                "0",
                "`bulk.max-requests-in-flight` must be greater than 0");
        assertOptionError(
                GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION.key(),
                "-1",
                "`bulk.allocator-init-reservation-bytes` must be greater than or equal to 0");
        assertOptionError(
                GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION.key(),
                "0",
                "`bulk.allocator-max-allocation-bytes` must be greater than 0");

        Map<String, String> options = baseOptions();
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION.key(), "2048");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION.key(), "1024");

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(options, "metrics"));
        assertEquals(
                "`bulk.allocator-max-allocation-bytes` must be greater than or equal to "
                        + "`bulk.allocator-init-reservation-bytes`",
                error.getMessage());
    }

    @Test
    void shouldValidateTimeIndexAgainstSchema() {
        Map<String, String> missingTimeIndex = baseOptions();
        missingTimeIndex.put(GreptimeConnectorOptions.TIME_INDEX.key(), "missing_ts");

        IllegalArgumentException missingError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(missingTimeIndex, "metrics"));
        assertEquals("Unknown time-index column: missing_ts", missingError.getMessage());

        Map<String, String> stringTimeIndex = baseOptions();
        stringTimeIndex.put(GreptimeConnectorOptions.TIME_INDEX.key(), "host");

        IllegalArgumentException typeError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(stringTimeIndex, "metrics"));
        assertEquals("time-index column must be TIMESTAMP or TIMESTAMP_LTZ, but was: STRING",
                typeError.getMessage());
    }

    @Test
    void shouldRejectNullableTimeIndex() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(baseOptions(), "metrics", resolvedSchemaWithNullableTimestamp()));

        assertEquals("time-index column must not be nullable: ts", error.getMessage());
    }

    @Test
    void shouldAcceptTimestampLtzTimeIndex() {
        Map<String, String> options = baseOptions();
        GreptimeDynamicTableSink sink = createSink(options, "metrics", resolvedSchemaWithTimestampLtz());

        assertEquals("ts", sink.options().timeIndex());
    }

    @Test
    void shouldValidateTagsAgainstSchema() {
        Map<String, String> missingTag = baseOptions();
        missingTag.put(GreptimeConnectorOptions.TAGS.key(), "host,missing_tag");

        IllegalArgumentException missingError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(missingTag, "metrics"));
        assertEquals("Unknown tag column: missing_tag", missingError.getMessage());

        Map<String, String> timeIndexTag = baseOptions();
        timeIndexTag.put(GreptimeConnectorOptions.TAGS.key(), "host,ts");

        IllegalArgumentException timeIndexError = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(timeIndexTag, "metrics"));
        assertEquals("Tag column cannot be the same as time-index: ts", timeIndexError.getMessage());
    }

    @Test
    void shouldCreateSinkV2RuntimeProvider() throws Exception {
        Map<String, String> options = baseOptions();
        options.put(GreptimeConnectorOptions.DATABASE.key(), "metrics_db");
        options.put(GreptimeConnectorOptions.USERNAME.key(), "greptime");
        options.put(GreptimeConnectorOptions.PASSWORD.key(), "secret");
        options.put(GreptimeConnectorOptions.TAGS.key(), "host,region");
        options.put(GreptimeConnectorOptions.BATCH_MAX_ROWS.key(), "128");
        options.put(GreptimeConnectorOptions.BULK_TIMEOUT_MS_PER_MESSAGE.key(), "30000");
        options.put(GreptimeConnectorOptions.BULK_MAX_REQUESTS_IN_FLIGHT.key(), "4");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_INIT_RESERVATION.key(), "1024");
        options.put(GreptimeConnectorOptions.BULK_ALLOCATOR_MAX_ALLOCATION.key(), "2048");

        GreptimeDynamicTableSink sink = createSink(options, "metrics");

        Sink<RowData> runtimeSink = assertRuntimeSinkSerializable(sink);
        assertRuntimeSinkSerializable(sink.copy());
        assertGreptimeSinkField(runtimeSink, "timeoutMsPerMessage", 30_000L);
        assertGreptimeSinkField(runtimeSink, "maxRequestsInFlight", 4);
        assertGreptimeSinkField(runtimeSink, "allocatorInitReservation", 1_024L);
        assertGreptimeSinkField(runtimeSink, "allocatorMaxAllocation", 2_048L);
        assertEquals("GreptimeDB Table Sink", sink.asSummaryString());
    }

    @Test
    void shouldCreateRuntimeProviderWithMultipleEndpoints() throws Exception {
        Map<String, String> options = baseOptions();
        options.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001,127.0.0.2:4001");

        GreptimeDynamicTableSink sink = createSink(options, "metrics");

        assertEquals(List.of("127.0.0.1:4001", "127.0.0.2:4001"), sink.options().endpoints());
        assertRuntimeSinkSerializable(sink);
    }

    private static Sink<RowData> assertRuntimeSinkSerializable(DynamicTableSink sink) throws Exception {
        SinkV2Provider provider = assertInstanceOf(SinkV2Provider.class, sink.getSinkRuntimeProvider(null));
        Sink<RowData> runtimeSink = provider.createSink();

        assertInstanceOf(GreptimeSink.class, runtimeSink);
        try (ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream())) {
            output.writeObject(runtimeSink);
        }
        return runtimeSink;
    }

    private static void assertGreptimeSinkField(Object sink, String fieldName, Object expectedValue) throws Exception {
        Field field = GreptimeSink.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        assertEquals(expectedValue, field.get(sink));
    }

    private static void assertOptionError(String key, String value, String expectedMessage) {
        Map<String, String> options = baseOptions();
        options.put(key, value);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> createSink(options, "metrics"));
        assertEquals(expectedMessage, error.getMessage());
    }

    private static GreptimeDynamicTableSink createSink(Map<String, String> options, String tableName) {
        return createSink(options, tableName, resolvedSchema());
    }

    private static GreptimeDynamicTableSink createSink(
            Map<String, String> options,
            String tableName,
            ResolvedSchema resolvedSchema) {
        Object sink = new GreptimeDynamicTableSinkFactory()
                .createDynamicTableSink(new TestFactoryContext(options, tableName, resolvedSchema));
        return assertInstanceOf(GreptimeDynamicTableSink.class, sink);
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(GreptimeConnectorOptions.ENDPOINTS.key(), "127.0.0.1:4001");
        options.put(GreptimeConnectorOptions.TIME_INDEX.key(), "ts");
        return options;
    }

    private static ResolvedSchema resolvedSchema() {
        return new ResolvedSchema(
                List.of(
                        Column.physical("host", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("usage", DataTypes.DOUBLE()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3).notNull())),
                List.of(),
                null);
    }

    private static ResolvedSchema resolvedSchemaWithTimestampLtz() {
        return new ResolvedSchema(
                List.of(
                        Column.physical("host", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("usage", DataTypes.DOUBLE()),
                        Column.physical("ts", DataTypes.TIMESTAMP_LTZ(3).notNull())),
                List.of(),
                null);
    }

    private static ResolvedSchema resolvedSchemaWithNullableTimestamp() {
        return new ResolvedSchema(
                List.of(
                        Column.physical("host", DataTypes.STRING()),
                        Column.physical("region", DataTypes.STRING()),
                        Column.physical("usage", DataTypes.DOUBLE()),
                        Column.physical("ts", DataTypes.TIMESTAMP(3))),
                List.of(),
                null);
    }

    private static final class TestFactoryContext implements DynamicTableFactory.Context {

        private final ObjectIdentifier objectIdentifier;
        private final ResolvedCatalogTable catalogTable;

        private TestFactoryContext(Map<String, String> options, String tableName, ResolvedSchema resolvedSchema) {
            CatalogTable table = CatalogTable.newBuilder()
                    .schema(Schema.newBuilder()
                            .fromResolvedSchema(resolvedSchema)
                            .build())
                    .options(options)
                    .build();
            this.objectIdentifier = ObjectIdentifier.of("default_catalog", "default_database", tableName);
            this.catalogTable = new ResolvedCatalogTable(table, resolvedSchema);
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
            return Map.of();
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
