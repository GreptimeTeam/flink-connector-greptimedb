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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.Objects;

final class GreptimeDynamicTableSource implements ScanTableSource {

    private final GreptimeJdbcQueryConfig queryConfig;
    private final DataType physicalRowDataType;
    private final String deferredValidationFailure;

    GreptimeDynamicTableSource(GreptimeJdbcQueryConfig queryConfig, DataType physicalRowDataType) {
        this(queryConfig, null, physicalRowDataType);
    }

    private GreptimeDynamicTableSource(
            GreptimeJdbcQueryConfig queryConfig,
            String deferredValidationFailure,
            DataType physicalRowDataType) {
        if ((queryConfig == null) == (deferredValidationFailure == null)) {
            throw new IllegalArgumentException(
                    "Exactly one of queryConfig and deferredValidationFailure must be set");
        }
        this.queryConfig = queryConfig;
        this.physicalRowDataType = Objects.requireNonNull(
                physicalRowDataType,
                "physicalRowDataType must not be null");
        this.deferredValidationFailure = deferredValidationFailure;
    }

    static GreptimeDynamicTableSource withDeferredValidationFailure(
            String deferredValidationFailure,
            DataType physicalRowDataType) {
        // Flink wraps factory errors with raw table options, so URL safety errors are deferred.
        return new GreptimeDynamicTableSource(
                null,
                Objects.requireNonNull(deferredValidationFailure, "deferredValidationFailure must not be null"),
                physicalRowDataType);
    }

    GreptimeJdbcQueryConfig queryConfig() {
        return queryConfig;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        if (deferredValidationFailure != null) {
            throw new ValidationException(deferredValidationFailure);
        }
        TypeInformation<RowData> producedType = context.createTypeInformation(physicalRowDataType);
        RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<String> columns = DataType.getFieldNames(physicalRowDataType);
        return InputFormatProvider.of(
                new GreptimeJdbcInputFormat(queryConfig, rowType, columns, producedType),
                1);
    }

    @Override
    public DynamicTableSource copy() {
        return deferredValidationFailure == null
                ? new GreptimeDynamicTableSource(queryConfig, physicalRowDataType)
                : withDeferredValidationFailure(deferredValidationFailure, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "GreptimeDB Bounded Table Source";
    }
}
