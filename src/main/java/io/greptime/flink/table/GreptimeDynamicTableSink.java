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
import io.greptime.models.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.Objects;

final class GreptimeDynamicTableSink implements DynamicTableSink {

    private final GreptimeTableSinkOptions options;
    private final ResolvedSchema schema;

    GreptimeDynamicTableSink(GreptimeTableSinkOptions options, ResolvedSchema schema) {
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
    }

    GreptimeTableSinkOptions options() {
        return options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        TableSchema tableSchema = GreptimeTableSchemaConverter.convert(schema, options);
        GreptimeRowDataSerializer serializer = GreptimeRowDataSerializer.create(schema, options.timeIndex());

        GreptimeSink.Builder<RowData> builder = GreptimeSink.<RowData>builder()
                .endpoints(options.endpoints())
                .database(options.database())
                .tableSchema(tableSchema)
                .recordSerializer(serializer)
                .batchSize(options.batchMaxRows())
                .timeoutMsPerMessage(options.bulkTimeoutMsPerMessage())
                .maxRequestsInFlight(options.bulkMaxRequestsInFlight())
                .allocatorMaxAllocation(options.bulkAllocatorMaxAllocation());

        if (options.bulkAllocatorInitReservation() > 0) {
            builder.allocatorInitReservation(options.bulkAllocatorInitReservation());
        }

        if (options.username() != null) {
            builder.plainTextAuth(options.username(), options.password());
        }

        return SinkV2Provider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new GreptimeDynamicTableSink(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "GreptimeDB Table Sink";
    }
}
