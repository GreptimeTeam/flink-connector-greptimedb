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

package io.greptime.flink.sink;

import java.io.Serializable;

/**
 * Converts Flink records into row values accepted by the GreptimeDB ingester.
 *
 * @param <T> the Flink record type to serialize
 */
@FunctionalInterface
public interface GreptimeRecordSerializer<T> extends Serializable {

    /**
     * Serializes a Flink record into a GreptimeDB row.
     *
     * @param element the Flink record to serialize
     * @return the row values in the same order as the configured table schema
     */
    Object[] serialize(T element);
}
