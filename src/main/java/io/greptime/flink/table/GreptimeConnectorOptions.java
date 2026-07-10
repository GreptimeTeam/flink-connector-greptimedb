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

import io.greptime.BulkWrite;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class GreptimeConnectorOptions {

    static final String IDENTIFIER = "greptimedb";
    static final String DEFAULT_DATABASE = "public";
    static final int DEFAULT_BATCH_MAX_ROWS = 1_000;
    static final long DEFAULT_BULK_TIMEOUT_MS_PER_MESSAGE = BulkWrite.DEFAULT_TIMEOUT_MS_PER_MESSAGE;
    static final int DEFAULT_BULK_MAX_REQUESTS_IN_FLIGHT = BulkWrite.DEFAULT_MAX_REQUESTS_IN_FLIGHT;
    static final long DEFAULT_BULK_ALLOCATOR_INIT_RESERVATION =
            BulkWrite.DEFAULT_ALLOCATOR_INIT_RESERVATION;
    static final long DEFAULT_BULK_ALLOCATOR_MAX_ALLOCATION =
            BulkWrite.DEFAULT_ALLOCATOR_MAX_ALLOCATION;

    static final ConfigOption<String> ENDPOINTS =
            ConfigOptions.key("endpoints").stringType().noDefaultValue();

    static final ConfigOption<String> TIME_INDEX =
            ConfigOptions.key("time-index").stringType().noDefaultValue();

    static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database").stringType().defaultValue(DEFAULT_DATABASE);

    static final ConfigOption<String> TABLE =
            ConfigOptions.key("table").stringType().noDefaultValue();

    static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    static final ConfigOption<String> TAGS =
            ConfigOptions.key("tags").stringType().defaultValue("");

    static final ConfigOption<Integer> BATCH_MAX_ROWS =
            ConfigOptions.key("batch.max-rows").intType().defaultValue(DEFAULT_BATCH_MAX_ROWS);

    static final ConfigOption<Long> BULK_TIMEOUT_MS_PER_MESSAGE =
            ConfigOptions.key("bulk.timeout-ms-per-message")
                    .longType()
                    .defaultValue(DEFAULT_BULK_TIMEOUT_MS_PER_MESSAGE);

    static final ConfigOption<Integer> BULK_MAX_REQUESTS_IN_FLIGHT =
            ConfigOptions.key("bulk.max-requests-in-flight")
                    .intType()
                    .defaultValue(DEFAULT_BULK_MAX_REQUESTS_IN_FLIGHT);

    static final ConfigOption<Long> BULK_ALLOCATOR_INIT_RESERVATION =
            ConfigOptions.key("bulk.allocator-init-reservation-bytes")
                    .longType()
                    .defaultValue(DEFAULT_BULK_ALLOCATOR_INIT_RESERVATION);

    static final ConfigOption<Long> BULK_ALLOCATOR_MAX_ALLOCATION =
            ConfigOptions.key("bulk.allocator-max-allocation-bytes")
                    .longType()
                    .defaultValue(DEFAULT_BULK_ALLOCATOR_MAX_ALLOCATION);

    private GreptimeConnectorOptions() {
    }

    static Set<ConfigOption<?>> requiredOptions() {
        return Set.of(ENDPOINTS, TIME_INDEX);
    }

    static Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                DATABASE,
                TABLE,
                USERNAME,
                PASSWORD,
                TAGS,
                BATCH_MAX_ROWS,
                BULK_TIMEOUT_MS_PER_MESSAGE,
                BULK_MAX_REQUESTS_IN_FLIGHT,
                BULK_ALLOCATOR_INIT_RESERVATION,
                BULK_ALLOCATOR_MAX_ALLOCATION);
    }

    static Set<ConfigOption<?>> forwardOptions() {
        return Set.of(ENDPOINTS, USERNAME, PASSWORD);
    }

    static GreptimeTableSinkOptions createSinkOptions(
            ReadableConfig options,
            String defaultTable,
            ResolvedSchema schema) {
        List<String> endpoints = splitListOption(ENDPOINTS.key(), options.get(ENDPOINTS), false);
        String timeIndex = options.get(TIME_INDEX);
        String database = options.get(DATABASE);
        String table = options.getOptional(TABLE).orElse(defaultTable);
        String username = options.getOptional(USERNAME).orElse(null);
        String password = options.getOptional(PASSWORD).orElse(null);
        List<String> tags = splitListOption(TAGS.key(), options.get(TAGS), true);
        int batchMaxRows = options.get(BATCH_MAX_ROWS);
        long bulkTimeoutMsPerMessage = options.get(BULK_TIMEOUT_MS_PER_MESSAGE);
        int bulkMaxRequestsInFlight = options.get(BULK_MAX_REQUESTS_IN_FLIGHT);
        long bulkAllocatorInitReservation = options.get(BULK_ALLOCATOR_INIT_RESERVATION);
        long bulkAllocatorMaxAllocation = options.get(BULK_ALLOCATOR_MAX_ALLOCATION);

        validateIdentifier(TIME_INDEX.key(), timeIndex);
        validateIdentifier(DATABASE.key(), database);
        validateIdentifier(TABLE.key(), table);
        validateAuthPair(username, password);
        validateTags(tags);
        validateSchema(schema, timeIndex, tags);
        validatePositive(BATCH_MAX_ROWS.key(), batchMaxRows);
        validatePositive(BULK_TIMEOUT_MS_PER_MESSAGE.key(), bulkTimeoutMsPerMessage);
        validatePositive(BULK_MAX_REQUESTS_IN_FLIGHT.key(), bulkMaxRequestsInFlight);
        validateNonNegative(BULK_ALLOCATOR_INIT_RESERVATION.key(), bulkAllocatorInitReservation);
        validatePositive(BULK_ALLOCATOR_MAX_ALLOCATION.key(), bulkAllocatorMaxAllocation);
        if (bulkAllocatorMaxAllocation < bulkAllocatorInitReservation) {
            throw new IllegalArgumentException(
                    "`" + BULK_ALLOCATOR_MAX_ALLOCATION.key() + "` must be greater than or equal to `"
                            + BULK_ALLOCATOR_INIT_RESERVATION.key() + "`");
        }

        return new GreptimeTableSinkOptions(
                endpoints,
                timeIndex,
                database,
                table,
                username,
                password,
                tags,
                batchMaxRows,
                bulkTimeoutMsPerMessage,
                bulkMaxRequestsInFlight,
                bulkAllocatorInitReservation,
                bulkAllocatorMaxAllocation);
    }

    private static void validatePositive(String key, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException("`" + key + "` must be greater than 0");
        }
    }

    private static void validateNonNegative(String key, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("`" + key + "` must be greater than or equal to 0");
        }
    }

    private static void validateAuthPair(String username, String password) {
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException("`username` and `password` must be configured together");
        }
        if (username != null) {
            validateRequiredText(USERNAME.key(), username);
            validateRequiredText(PASSWORD.key(), password);
        }
    }

    private static void validateTags(List<String> tags) {
        Set<String> seen = new HashSet<>();
        for (String tag : tags) {
            if (!seen.add(tag)) {
                throw new IllegalArgumentException("Duplicate tag column: " + tag);
            }
        }
    }

    private static List<String> splitListOption(String key, String value, boolean allowEmpty) {
        if (StringUtils.isBlank(value)) {
            if (allowEmpty) {
                return List.of();
            }
            throw new IllegalArgumentException("`" + key + "` must not be blank");
        }

        List<String> values = new ArrayList<>();
        for (String rawValue : value.split(",", -1)) {
            String trimmedValue = rawValue.trim();
            validateRequiredText(key, trimmedValue);
            values.add(trimmedValue);
        }
        return values;
    }

    private static void validateSchema(ResolvedSchema schema, String timeIndex, List<String> tags) {
        Column timeIndexColumn = schema.getColumn(timeIndex)
                .orElseThrow(() -> new IllegalArgumentException("Unknown time-index column: " + timeIndex));
        LogicalType timeIndexType = timeIndexColumn.getDataType().getLogicalType();
        if (!isTimestampType(timeIndexType)) {
            throw new IllegalArgumentException(
                    "time-index column must be TIMESTAMP or TIMESTAMP_LTZ, but was: " + timeIndexType);
        }
        if (timeIndexType.isNullable()) {
            throw new IllegalArgumentException("time-index column must not be nullable: " + timeIndex);
        }

        for (String tag : tags) {
            if (tag.equals(timeIndex)) {
                throw new IllegalArgumentException("Tag column cannot be the same as time-index: " + tag);
            }
            schema.getColumn(tag)
                    .orElseThrow(() -> new IllegalArgumentException("Unknown tag column: " + tag));
        }
    }

    private static boolean isTimestampType(LogicalType type) {
        LogicalTypeRoot root = type.getTypeRoot();
        return root == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                || root == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static void validateIdentifier(String key, String value) {
        validateRequiredText(key, value);
        if (!Objects.equals(value, value.trim())) {
            throw new IllegalArgumentException("`" + key + "` must not have leading or trailing whitespace");
        }
    }

    private static void validateRequiredText(String key, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("`" + key + "` must not be blank");
        }
    }
}
