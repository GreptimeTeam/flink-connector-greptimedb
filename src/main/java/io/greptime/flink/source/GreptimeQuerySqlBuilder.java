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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

final class GreptimeQuerySqlBuilder {

    private GreptimeQuerySqlBuilder() {
    }

    static String buildSelect(String database, String table, List<String> columns) {
        Objects.requireNonNull(database, "database must not be null");
        Objects.requireNonNull(table, "table must not be null");
        Objects.requireNonNull(columns, "columns must not be null");
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }

        String selectList = columns.stream()
                .map(GreptimeQuerySqlBuilder::quoteIdentifier)
                .collect(Collectors.joining(", "));
        return "SELECT " + selectList + " FROM " + quoteIdentifier(database) + "." + quoteIdentifier(table);
    }

    private static String quoteIdentifier(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
