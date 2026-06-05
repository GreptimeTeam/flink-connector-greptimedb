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

import io.greptime.models.DataType;
import io.greptime.models.SemanticType;
import io.greptime.models.TableSchema;
import io.greptime.v1.Common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Serializable table schema descriptor used in {@link GreptimeSink}.
 *
 * <p>
 * The GreptimeDB ingester {@link TableSchema} does not implement Java
 * serialization, but Flink serializes sink instances when building and
 * distributing a job graph. This class stores only the schema fields required
 * to rebuild the ingester schema on the task side.
 */
final class GreptimeTableSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final List<String> columnNames;
    private final List<SemanticType> semanticTypes;
    private final List<DataType> dataTypes;
    private final List<DecimalTypeExtension> decimalTypeExtensions;

    private GreptimeTableSchema(
            String tableName,
            List<String> columnNames,
            List<SemanticType> semanticTypes,
            List<DataType> dataTypes,
            List<DecimalTypeExtension> decimalTypeExtensions) {
        this.tableName = tableName;
        this.columnNames = copy(columnNames);
        this.semanticTypes = copy(semanticTypes);
        this.dataTypes = copy(dataTypes);
        this.decimalTypeExtensions = copy(decimalTypeExtensions);
    }

    /**
     * Creates a serializable descriptor from an ingester table schema.
     *
     * @param tableSchema the ingester table schema
     * @return a serializable schema descriptor
     */
    static GreptimeTableSchema from(TableSchema tableSchema) {
        List<String> columnNames = tableSchema.getColumnNames();
        List<SemanticType> semanticTypes = new ArrayList<>(columnNames.size());
        List<DataType> dataTypes = new ArrayList<>(columnNames.size());
        List<DecimalTypeExtension> decimalTypeExtensions = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            semanticTypes.add(toSemanticType(tableSchema.getSemanticTypes().get(i)));
            dataTypes.add(toDataType(tableSchema.getDataTypes().get(i)));
            decimalTypeExtensions.add(fromDecimalTypeExtension(tableSchema.getDataTypeExtensions().get(i)));
        }

        return new GreptimeTableSchema(
                tableSchema.getTableName(),
                columnNames,
                semanticTypes,
                dataTypes,
                decimalTypeExtensions);
    }

    /**
     * Rebuilds the ingester table schema from this descriptor.
     *
     * @return a table schema accepted by the GreptimeDB ingester client
     */
    TableSchema toTableSchema() {
        TableSchema.Builder builder = TableSchema.newBuilder(tableName);
        for (int i = 0; i < columnNames.size(); i++) {
            builder.addColumn(
                    columnNames.get(i),
                    semanticTypes.get(i),
                    dataTypes.get(i),
                    toDecimalTypeExtension(decimalTypeExtensions.get(i)));
        }
        return builder.build();
    }

    static <T> List<T> copy(List<T> values) {
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    static SemanticType toSemanticType(Common.SemanticType semanticType) {
        switch (semanticType) {
            case TAG:
                return SemanticType.Tag;
            case FIELD:
                return SemanticType.Field;
            case TIMESTAMP:
                return SemanticType.Timestamp;
            default:
                throw new IllegalArgumentException("Unsupported semantic type: " + semanticType);
        }
    }

    static DataType toDataType(Common.ColumnDataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return DataType.Bool;
            case INT8:
                return DataType.Int8;
            case INT16:
                return DataType.Int16;
            case INT32:
                return DataType.Int32;
            case INT64:
                return DataType.Int64;
            case UINT8:
                return DataType.UInt8;
            case UINT16:
                return DataType.UInt16;
            case UINT32:
                return DataType.UInt32;
            case UINT64:
                return DataType.UInt64;
            case FLOAT32:
                return DataType.Float32;
            case FLOAT64:
                return DataType.Float64;
            case BINARY:
                return DataType.Binary;
            case STRING:
                return DataType.String;
            case DATE:
                return DataType.Date;
            case TIMESTAMP_SECOND:
                return DataType.TimestampSecond;
            case TIMESTAMP_MILLISECOND:
                return DataType.TimestampMillisecond;
            case TIMESTAMP_MICROSECOND:
                return DataType.TimestampMicrosecond;
            case TIMESTAMP_NANOSECOND:
                return DataType.TimestampNanosecond;
            case TIME_SECOND:
                return DataType.TimeSecond;
            case TIME_MILLISECOND:
                return DataType.TimeMilliSecond;
            case TIME_MICROSECOND:
                return DataType.TimeMicroSecond;
            case TIME_NANOSECOND:
                return DataType.TimeNanoSecond;
            case DECIMAL128:
                return DataType.Decimal128;
            case JSON:
                return DataType.Json;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }

    static DecimalTypeExtension fromDecimalTypeExtension(
            Common.ColumnDataTypeExtension dataTypeExtension) {
        if (dataTypeExtension == null || !dataTypeExtension.hasDecimalType()) {
            return null;
        }

        Common.DecimalTypeExtension decimalType = dataTypeExtension.getDecimalType();
        return new DecimalTypeExtension(
                decimalType.getPrecision(),
                decimalType.getScale());
    }

    static DataType.DecimalTypeExtension toDecimalTypeExtension(DecimalTypeExtension decimalTypeExtension) {
        if (decimalTypeExtension == null) {
            return null;
        }
        return new DataType.DecimalTypeExtension(
                decimalTypeExtension.precision,
                decimalTypeExtension.scale);
    }

    /**
     * Serializable representation of decimal type metadata.
     *
     * <p>
     * The ingester {@link DataType.DecimalTypeExtension} type does not implement
     * {@link Serializable}, so the descriptor stores its primitive fields instead.
     */
    static final class DecimalTypeExtension implements Serializable {

        private static final long serialVersionUID = 1L;

        private final int precision;
        private final int scale;

        private DecimalTypeExtension(int precision, int scale) {
            this.precision = precision;
            this.scale = scale;
        }
    }
}
