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

import java.io.Serializable;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class GreptimeJdbcQueryConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String MYSQL_JDBC_PREFIX = "jdbc:mysql:";
    private static final String SENSITIVE_JDBC_URL_MESSAGE =
            "`query.jdbc-url` must not contain credentials or authentication tokens; "
                    + "use `username` and `password` options instead";
    private static final String MALFORMED_JDBC_URL_MESSAGE =
            "Invalid percent-encoding in `query.jdbc-url`";
    private static final List<String> SENSITIVE_QUERY_KEY_FRAGMENTS =
            List.of("password", "passwd", "pwd", "token", "secret", "apikey", "auth", "credential");
    private static final Pattern AUTHORITY_PROPERTY_KEY_PATTERN =
            Pattern.compile("(?:^|[,(])\\s*([\\w.%-]+)\\s*=");

    private final String jdbcUrl;
    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final int connectTimeoutMs;
    private final int socketTimeoutMs;
    private final int fetchSize;

    GreptimeJdbcQueryConfig(
            String jdbcUrl,
            String database,
            String table,
            String username,
            String password,
            int connectTimeoutMs,
            int socketTimeoutMs,
            int fetchSize) {
        this.jdbcUrl = validateJdbcUrl(jdbcUrl);
        this.database = database;
        this.table = table;
        this.username = username;
        this.password = password;
        this.connectTimeoutMs = connectTimeoutMs;
        this.socketTimeoutMs = socketTimeoutMs;
        this.fetchSize = fetchSize;
    }

    String jdbcUrl() {
        return jdbcUrl;
    }

    String database() {
        return database;
    }

    String table() {
        return table;
    }

    int fetchSize() {
        return fetchSize;
    }

    Properties connectionProperties() {
        Properties properties = new Properties();
        properties.setProperty("connectTimeout", Integer.toString(connectTimeoutMs));
        properties.setProperty("socketTimeout", Integer.toString(socketTimeoutMs));
        if (username != null) {
            properties.setProperty("user", username);
            properties.setProperty("password", password);
        }
        return properties;
    }

    static String deferredValidationFailure(String jdbcUrl) {
        try {
            validatePercentEncoding(jdbcUrl);
            if (hasAuthorityUserInfo(jdbcUrl)) {
                return SENSITIVE_JDBC_URL_MESSAGE;
            }
            for (String key : jdbcUrlPropertyKeys(jdbcUrl)) {
                if (isSensitiveQueryKey(key)) {
                    return SENSITIVE_JDBC_URL_MESSAGE;
                }
            }
            return null;
        } catch (IllegalArgumentException e) {
            return MALFORMED_JDBC_URL_MESSAGE;
        }
    }

    private static String validateJdbcUrl(String jdbcUrl) {
        if (StringUtils.isBlank(jdbcUrl)) {
            throw new IllegalArgumentException("`query.jdbc-url` must not be blank");
        }
        if (!jdbcUrl.equals(jdbcUrl.trim())) {
            throw new IllegalArgumentException("`query.jdbc-url` must not have leading or trailing whitespace");
        }
        if (!jdbcUrl.toLowerCase(Locale.ROOT).startsWith(MYSQL_JDBC_PREFIX)) {
            throw new IllegalArgumentException("GreptimeDB source supports only MySQL JDBC URLs");
        }
        String deferredFailure = deferredValidationFailure(jdbcUrl);
        if (deferredFailure != null) {
            throw new IllegalArgumentException(deferredFailure);
        }

        for (String key : jdbcUrlPropertyKeys(jdbcUrl)) {
            if ("connecttimeout".equals(key) || "sockettimeout".equals(key)) {
                throw new IllegalArgumentException(
                        "`query.jdbc-url` must not configure JDBC timeouts; use the typed timeout options instead");
            }
        }
        return jdbcUrl;
    }

    private static List<String> jdbcUrlPropertyKeys(String jdbcUrl) {
        List<String> keys = new ArrayList<>();
        keys.addAll(authorityPropertyKeys(jdbcUrl));
        keys.addAll(queryParameterKeys(jdbcUrl));
        return keys;
    }

    private static List<String> authorityPropertyKeys(String jdbcUrl) {
        int authorityStart = jdbcUrl.indexOf("://");
        if (authorityStart < 0) {
            return List.of();
        }
        authorityStart += 3;
        int authorityEnd = authorityEnd(jdbcUrl, authorityStart);
        String authority = decodeUrlComponent(jdbcUrl.substring(authorityStart, authorityEnd));
        List<String> keys = new ArrayList<>();
        Matcher matcher = AUTHORITY_PROPERTY_KEY_PATTERN.matcher(authority);
        while (matcher.find()) {
            keys.add(matcher.group(1).toLowerCase(Locale.ROOT));
        }
        return keys;
    }

    private static List<String> queryParameterKeys(String jdbcUrl) {
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart == jdbcUrl.length() - 1) {
            return List.of();
        }
        int fragmentStart = jdbcUrl.indexOf('#', queryStart + 1);
        String query = fragmentStart < 0
                ? jdbcUrl.substring(queryStart + 1)
                : jdbcUrl.substring(queryStart + 1, fragmentStart);
        return java.util.Arrays.stream(query.split("&"))
                .filter(parameter -> !parameter.isEmpty())
                .map(GreptimeJdbcQueryConfig::parseQueryParameter)
                .toList();
    }

    private static String parseQueryParameter(String parameter) {
        int valueStart = parameter.indexOf('=');
        String rawKey = valueStart < 0 ? parameter : parameter.substring(0, valueStart);
        String key = decodeUrlComponent(rawKey).toLowerCase(Locale.ROOT);
        if (valueStart >= 0) {
            decodeUrlComponent(parameter.substring(valueStart + 1));
        }
        return key;
    }

    private static void validatePercentEncoding(String jdbcUrl) {
        decodeUrlComponent(jdbcUrl);
    }

    private static String decodeUrlComponent(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(MALFORMED_JDBC_URL_MESSAGE, e);
        }
    }

    private static boolean hasAuthorityUserInfo(String jdbcUrl) {
        int authorityStart = jdbcUrl.indexOf("://");
        if (authorityStart < 0) {
            return false;
        }
        authorityStart += 3;
        int authorityEnd = authorityEnd(jdbcUrl, authorityStart);
        String authority = decodeUrlComponent(jdbcUrl.substring(authorityStart, authorityEnd));
        return authority.indexOf('@') >= 0;
    }

    private static int authorityEnd(String jdbcUrl, int authorityStart) {
        int authorityEnd = jdbcUrl.length();
        for (char delimiter : new char[] {'/', '?', '#'}) {
            int index = jdbcUrl.indexOf(delimiter, authorityStart);
            if (index >= 0 && index < authorityEnd) {
                authorityEnd = index;
            }
        }
        return authorityEnd;
    }

    private static boolean isSensitiveQueryKey(String key) {
        if ("user".equals(key) || "username".equals(key)) {
            return true;
        }
        String compactKey = key.replace("_", "").replace("-", "").replace(".", "");
        return SENSITIVE_QUERY_KEY_FRAGMENTS.stream()
                .anyMatch(fragment -> key.contains(fragment) || compactKey.contains(fragment));
    }
}
