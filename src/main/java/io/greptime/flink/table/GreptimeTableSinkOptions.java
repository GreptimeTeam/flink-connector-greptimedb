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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class GreptimeTableSinkOptions {

    private final List<String> endpoints;
    private final String timeIndex;
    private final String database;
    private final String table;
    private final String username;
    private final String password;
    private final List<String> tags;
    private final int batchMaxRows;

    GreptimeTableSinkOptions(
            List<String> endpoints,
            String timeIndex,
            String database,
            String table,
            String username,
            String password,
            List<String> tags,
            int batchMaxRows) {
        this.endpoints = copy(endpoints);
        this.timeIndex = Objects.requireNonNull(timeIndex, "timeIndex must not be null");
        this.database = Objects.requireNonNull(database, "database must not be null");
        this.table = Objects.requireNonNull(table, "table must not be null");
        this.username = username;
        this.password = password;
        this.tags = copy(tags);
        this.batchMaxRows = batchMaxRows;
    }

    List<String> endpoints() {
        return endpoints;
    }

    String timeIndex() {
        return timeIndex;
    }

    String database() {
        return database;
    }

    String table() {
        return table;
    }

    String username() {
        return username;
    }

    String password() {
        return password;
    }

    List<String> tags() {
        return tags;
    }

    int batchMaxRows() {
        return batchMaxRows;
    }

    private static <T> List<T> copy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }
}
