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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Objects;
import java.util.Set;

final class GreptimeSourceOptions {

    static final String IDENTIFIER = "greptimedb";
    static final String DEFAULT_DATABASE = "public";
    static final int DEFAULT_CONNECT_TIMEOUT_MS = 10_000;
    static final int DEFAULT_SOCKET_TIMEOUT_MS = 300_000;
    static final int DEFAULT_FETCH_SIZE = 0;

    static final ConfigOption<String> JDBC_URL =
            ConfigOptions.key("query.jdbc-url").stringType().noDefaultValue();

    static final ConfigOption<String> DATABASE =
            ConfigOptions.key("database").stringType().defaultValue(DEFAULT_DATABASE);

    static final ConfigOption<String> TABLE =
            ConfigOptions.key("table").stringType().noDefaultValue();

    static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    static final ConfigOption<Integer> CONNECT_TIMEOUT_MS = ConfigOptions.key("query.connect-timeout-ms")
            .intType()
            .defaultValue(DEFAULT_CONNECT_TIMEOUT_MS);

    static final ConfigOption<Integer> SOCKET_TIMEOUT_MS = ConfigOptions.key("query.socket-timeout-ms")
            .intType()
            .defaultValue(DEFAULT_SOCKET_TIMEOUT_MS);

    static final ConfigOption<Integer> FETCH_SIZE = ConfigOptions.key("query.fetch-size")
            .intType()
            .defaultValue(DEFAULT_FETCH_SIZE);

    private GreptimeSourceOptions() {
    }

    static Set<ConfigOption<?>> requiredOptions() {
        return Set.of(JDBC_URL);
    }

    static Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                DATABASE,
                TABLE,
                USERNAME,
                PASSWORD,
                CONNECT_TIMEOUT_MS,
                SOCKET_TIMEOUT_MS,
                FETCH_SIZE);
    }

    static Set<ConfigOption<?>> forwardOptions() {
        return Set.of(JDBC_URL, USERNAME, PASSWORD, CONNECT_TIMEOUT_MS, SOCKET_TIMEOUT_MS);
    }

    static GreptimeJdbcQueryConfig createQueryConfig(ReadableConfig options, String defaultTable) {
        String database = options.get(DATABASE);
        String table = options.getOptional(TABLE).orElse(defaultTable);
        String username = options.getOptional(USERNAME).orElse(null);
        String password = options.getOptional(PASSWORD).orElse(null);
        int connectTimeoutMs = options.get(CONNECT_TIMEOUT_MS);
        int socketTimeoutMs = options.get(SOCKET_TIMEOUT_MS);
        int fetchSize = options.get(FETCH_SIZE);

        validateIdentifier(DATABASE.key(), database);
        validateIdentifier(TABLE.key(), table);
        validateAuthPair(username, password);
        validatePositive(CONNECT_TIMEOUT_MS.key(), connectTimeoutMs);
        validatePositive(SOCKET_TIMEOUT_MS.key(), socketTimeoutMs);
        validateNonNegative(FETCH_SIZE.key(), fetchSize);

        return new GreptimeJdbcQueryConfig(
                options.get(JDBC_URL),
                database,
                table,
                username,
                password,
                connectTimeoutMs,
                socketTimeoutMs,
                fetchSize);
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

    private static void validateIdentifier(String key, String value) {
        validateRequiredText(key, value);
        if (!Objects.equals(value, value.trim())) {
            throw new IllegalArgumentException("`" + key + "` must not have leading or trailing whitespace");
        }
    }

    private static void validatePositive(String key, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException("`" + key + "` must be greater than 0");
        }
    }

    private static void validateNonNegative(String key, int value) {
        if (value < 0) {
            throw new IllegalArgumentException("`" + key + "` must be greater than or equal to 0");
        }
    }

    private static void validateRequiredText(String key, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("`" + key + "` must not be blank");
        }
    }
}
