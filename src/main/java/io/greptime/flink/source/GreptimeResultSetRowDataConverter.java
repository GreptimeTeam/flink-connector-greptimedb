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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Objects;

final class GreptimeResultSetRowDataConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final RowType rowType;

    GreptimeResultSetRowDataConverter(RowType rowType) {
        this.rowType = Objects.requireNonNull(rowType, "rowType must not be null");
        validate(rowType);
    }

    static void validate(RowType rowType) {
        for (RowType.RowField field : rowType.getFields()) {
            switch (field.getType().getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DATE:
                case DECIMAL:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    break;
                default:
                    throw unsupportedType(field);
            }
        }
    }

    RowData convert(ResultSet resultSet) throws SQLException {
        Objects.requireNonNull(resultSet, "resultSet must not be null");
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            row.setField(i, readField(resultSet, i + 1, rowType.getFields().get(i)));
        }
        return row;
    }

    private static Object readField(ResultSet resultSet, int position, RowType.RowField field)
            throws SQLException {
        LogicalType type = field.getType();
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                boolean booleanValue = resultSet.getBoolean(position);
                return resultSet.wasNull() ? null : booleanValue;
            case TINYINT:
                byte byteValue = resultSet.getByte(position);
                return resultSet.wasNull() ? null : byteValue;
            case SMALLINT:
                short shortValue = resultSet.getShort(position);
                return resultSet.wasNull() ? null : shortValue;
            case INTEGER:
                int intValue = resultSet.getInt(position);
                return resultSet.wasNull() ? null : intValue;
            case BIGINT:
                long longValue = resultSet.getLong(position);
                return resultSet.wasNull() ? null : longValue;
            case FLOAT:
                float floatValue = resultSet.getFloat(position);
                return resultSet.wasNull() ? null : floatValue;
            case DOUBLE:
                double doubleValue = resultSet.getDouble(position);
                return resultSet.wasNull() ? null : doubleValue;
            case CHAR:
            case VARCHAR:
                String stringValue = resultSet.getString(position);
                return stringValue == null ? null : StringData.fromString(stringValue);
            case BINARY:
            case VARBINARY:
                return resultSet.getBytes(position);
            case DATE:
                Date dateValue = resultSet.getDate(position);
                return dateValue == null ? null : (int) dateValue.toLocalDate().toEpochDay();
            case DECIMAL:
                return decimalValue(resultSet, position, field, (DecimalType) type);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Timestamp timestampValue = resultSet.getTimestamp(position);
                return timestampValue == null ? null : TimestampData.fromTimestamp(timestampValue);
            default:
                throw unsupportedType(field);
        }
    }

    private static DecimalData decimalValue(
            ResultSet resultSet,
            int position,
            RowType.RowField field,
            DecimalType type) throws SQLException {
        BigDecimal value = resultSet.getBigDecimal(position);
        if (value == null) {
            return null;
        }
        DecimalData decimal = DecimalData.fromBigDecimal(value, type.getPrecision(), type.getScale());
        if (decimal == null) {
            throw new SQLException(
                    "DECIMAL value exceeds declared precision and scale for column `" + field.getName() + "`");
        }
        return decimal;
    }

    private static IllegalArgumentException unsupportedType(RowType.RowField field) {
        return new IllegalArgumentException(
                "Unsupported Flink logical type for GreptimeDB source column `"
                        + field.getName()
                        + "`: "
                        + field.getType());
    }
}
