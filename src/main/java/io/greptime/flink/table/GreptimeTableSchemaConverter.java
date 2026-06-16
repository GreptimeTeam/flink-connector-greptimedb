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

import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import io.greptime.models.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class GreptimeTableSchemaConverter {

    private GreptimeTableSchemaConverter() {
    }

    static TableSchema convert(ResolvedSchema schema, GreptimeTableSinkOptions options) {
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(options, "options must not be null");

        TableSchema.Builder builder = TableSchema.newBuilder(options.table());
        for (Column column : physicalColumns(schema)) {
            String columnName = column.getName();
            LogicalType logicalType = column.getDataType().getLogicalType();
            SemanticType semanticType = semanticType(columnName, options);
            DataType dataType = dataType(columnName, logicalType);
            if (semanticType == SemanticType.Timestamp) {
                validateTimeIndexColumn(columnName, logicalType, dataType);
            }
            builder.addColumn(columnName, semanticType, dataType, decimalExtension(logicalType));
        }
        return builder.build();
    }

    static List<Column> physicalColumns(ResolvedSchema schema) {
        List<Column> physicalColumns = new ArrayList<>(schema.getColumnCount());
        for (Column column : schema.getColumns()) {
            if (!column.isPhysical()) {
                throw new IllegalArgumentException(
                        "GreptimeDB Table sink only supports physical columns, but found: " + column.getName());
            }
            physicalColumns.add(column);
        }
        return physicalColumns;
    }

    static IllegalArgumentException unsupportedType(String columnName, LogicalType logicalType) {
        return new IllegalArgumentException(
                "Unsupported Flink logical type for GreptimeDB column `" + columnName + "`: " + logicalType);
    }

    private static SemanticType semanticType(String columnName, GreptimeTableSinkOptions options) {
        if (columnName.equals(options.timeIndex())) {
            return SemanticType.Timestamp;
        }
        if (options.tags().contains(columnName)) {
            return SemanticType.Tag;
        }
        return SemanticType.Field;
    }

    private static void validateTimeIndexColumn(String columnName, LogicalType logicalType, DataType dataType) {
        if (!dataType.isTimestamp()) {
            throw new IllegalArgumentException(
                    "time-index column must map to a GreptimeDB timestamp type: " + columnName);
        }
        if (logicalType.isNullable()) {
            throw new IllegalArgumentException("time-index column must not be nullable: " + columnName);
        }
    }

    private static DataType dataType(String columnName, LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return DataType.Bool;
            case TINYINT:
                return DataType.Int8;
            case SMALLINT:
                return DataType.Int16;
            case INTEGER:
                return DataType.Int32;
            case BIGINT:
                return DataType.Int64;
            case FLOAT:
                return DataType.Float32;
            case DOUBLE:
                return DataType.Float64;
            case CHAR:
            case VARCHAR:
                return DataType.String;
            case BINARY:
            case VARBINARY:
                return DataType.Binary;
            case DATE:
                return DataType.Date;
            case TIME_WITHOUT_TIME_ZONE:
                return timeDataType((TimeType) logicalType);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampDataType(((TimestampType) logicalType).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampDataType(((LocalZonedTimestampType) logicalType).getPrecision());
            case DECIMAL:
                return DataType.Decimal128;
            default:
                throw unsupportedType(columnName, logicalType);
        }
    }

    private static DataType timeDataType(TimeType timeType) {
        int precision = timeType.getPrecision();
        if (precision == 0) {
            return DataType.TimeSecond;
        }
        if (precision <= 3) {
            return DataType.TimeMilliSecond;
        }
        if (precision <= 6) {
            return DataType.TimeMicroSecond;
        }
        return DataType.TimeNanoSecond;
    }

    private static DataType timestampDataType(int precision) {
        if (precision == 0) {
            return DataType.TimestampSecond;
        }
        if (precision <= 3) {
            return DataType.TimestampMillisecond;
        }
        if (precision <= 6) {
            return DataType.TimestampMicrosecond;
        }
        return DataType.TimestampNanosecond;
    }

    private static DataType.DecimalTypeExtension decimalExtension(LogicalType logicalType) {
        if (!(logicalType instanceof DecimalType)) {
            return null;
        }
        DecimalType decimalType = (DecimalType) logicalType;
        return new DataType.DecimalTypeExtension(decimalType.getPrecision(), decimalType.getScale());
    }
}
