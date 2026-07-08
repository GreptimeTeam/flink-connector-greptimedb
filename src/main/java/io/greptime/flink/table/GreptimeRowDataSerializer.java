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

import io.greptime.flink.sink.GreptimeRecordSerializer;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;

final class GreptimeRowDataSerializer implements GreptimeRecordSerializer<RowData> {

    private static final long serialVersionUID = 1L;

    private final FieldConverter[] converters;

    private GreptimeRowDataSerializer(FieldConverter[] converters) {
        this.converters = converters;
    }

    /**
     * Creates a serializer for the given schema without time-index null enforcement.
     *
     * @param schema the resolved schema
     * @return a new serializer
     */
    static GreptimeRowDataSerializer create(ResolvedSchema schema) {
        return create(schema, null);
    }

    /**
     * Creates a serializer for the given schema with time-index null enforcement.
     *
     * @param schema          the resolved schema
     * @param timeIndexColumn the name of the time-index column, or {@code null} to
     *                        skip null enforcement
     * @return a new serializer
     */
    static GreptimeRowDataSerializer create(ResolvedSchema schema, String timeIndexColumn) {
        Objects.requireNonNull(schema, "schema must not be null");
        List<Column> columns = GreptimeTableSchemaConverter.physicalColumns(schema);
        FieldConverter[] converters = new FieldConverter[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            converters[i] = createFieldConverter(
                    column.getName(), column.getDataType().getLogicalType(), i, timeIndexColumn);
        }
        return new GreptimeRowDataSerializer(converters);
    }

    @Override
    public Object[] serialize(RowData row) {
        Objects.requireNonNull(row, "row must not be null");
        if (row.getRowKind() != RowKind.INSERT) {
            throw new IllegalArgumentException("GreptimeDB Table sink only supports INSERT rows");
        }
        if (row.getArity() != converters.length) {
            throw new IllegalArgumentException(
                    "Expected RowData arity: " + converters.length + ", actual: " + row.getArity());
        }

        Object[] values = new Object[converters.length];
        for (int i = 0; i < converters.length; i++) {
            values[i] = converters[i].convert(row);
        }
        return values;
    }

    private static FieldConverter createFieldConverter(
            String columnName, LogicalType logicalType, int index, String timeIndexColumn) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return wrap(columnName, index, timeIndexColumn, row -> row.getBoolean(index));
            case TINYINT:
                return wrap(columnName, index, timeIndexColumn, row -> (int) row.getByte(index));
            case SMALLINT:
                return wrap(columnName, index, timeIndexColumn, row -> (int) row.getShort(index));
            case INTEGER:
                return wrap(columnName, index, timeIndexColumn, row -> row.getInt(index));
            case BIGINT:
                return wrap(columnName, index, timeIndexColumn, row -> row.getLong(index));
            case FLOAT:
                return wrap(columnName, index, timeIndexColumn, row -> row.getFloat(index));
            case DOUBLE:
                return wrap(columnName, index, timeIndexColumn, row -> row.getDouble(index));
            case CHAR:
            case VARCHAR:
                return wrap(columnName, index, timeIndexColumn, row -> row.getString(index).toString());
            case BINARY:
            case VARBINARY:
                return wrap(columnName, index, timeIndexColumn, row -> row.getBinary(index));
            case DATE:
                return wrap(columnName, index, timeIndexColumn, row -> row.getInt(index));
            case TIME_WITHOUT_TIME_ZONE:
                return timeConverter(columnName, index, ((TimeType) logicalType).getPrecision(), timeIndexColumn);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return timestampConverter(
                        columnName, index, ((TimestampType) logicalType).getPrecision(), false, timeIndexColumn);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return timestampConverter(
                        columnName,
                        index,
                        ((LocalZonedTimestampType) logicalType).getPrecision(),
                        true,
                        timeIndexColumn);
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return wrap(columnName, index, timeIndexColumn, row -> toBigDecimal(row.getDecimal(
                        index,
                        decimalType.getPrecision(),
                        decimalType.getScale())));
            default:
                throw GreptimeTableSchemaConverter.unsupportedType(columnName, logicalType);
        }
    }

    private static FieldConverter timeConverter(
            String columnName, int index, int precision, String timeIndexColumn) {
        return wrap(columnName, index, timeIndexColumn,
                row -> convertTime(row.getInt(index), precision));
    }

    private static FieldConverter timestampConverter(
            String columnName,
            int index,
            int precision,
            boolean localTimeZone,
            String timeIndexColumn) {
        return wrap(columnName, index, timeIndexColumn,
                row -> convertTimestamp(
                        columnName,
                        row.getTimestamp(index, precision),
                        precision,
                        localTimeZone));
    }

    private static Object convertTime(int millisOfDay, int precision) {
        if (precision == 0) {
            return millisOfDay / 1_000;
        }
        if (precision <= 3) {
            return millisOfDay;
        }
        if (precision <= 6) {
            return Math.multiplyExact((long) millisOfDay, 1_000L);
        }
        return Math.multiplyExact((long) millisOfDay, 1_000_000L);
    }

    private static Object convertTimestamp(
            String columnName,
            TimestampData timestamp,
            int precision,
            boolean localTimeZone) {
        Instant instant = localTimeZone
                ? timestamp.toInstant()
                : timestamp.toLocalDateTime().toInstant(ZoneOffset.UTC);
        try {
            if (precision == 0) {
                return instant.getEpochSecond();
            }
            if (precision <= 3) {
                return instant.toEpochMilli();
            }
            if (precision <= 6) {
                return toEpochMicros(instant);
            }
            return toEpochNanos(instant);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Timestamp value out of range for GreptimeDB column `"
                            + columnName
                            + "` with precision "
                            + precision,
                    e);
        }
    }

    private static long toEpochMicros(Instant instant) {
        return Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000L),
                instant.getNano() / 1_000L);
    }

    private static long toEpochNanos(Instant instant) {
        return Math.addExact(
                Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L),
                instant.getNano());
    }

    private static Object toBigDecimal(DecimalData decimal) {
        return decimal.toBigDecimal();
    }

    private static FieldConverter wrap(
            String columnName, int index, String timeIndexColumn, FieldConverter converter) {
        if (columnName.equals(timeIndexColumn)) {
            return required(columnName, index, converter);
        }
        return nullable(index, converter);
    }

    private static FieldConverter required(String columnName, int index, FieldConverter converter) {
        return row -> {
            if (row.isNullAt(index)) {
                throw new IllegalArgumentException(
                        "time-index column must not be null: " + columnName);
            }
            return converter.convert(row);
        };
    }

    private static FieldConverter nullable(int index, FieldConverter converter) {
        return row -> row.isNullAt(index) ? null : converter.convert(row);
    }

    @FunctionalInterface
    private interface FieldConverter extends Serializable {

        Object convert(RowData row);
    }
}
