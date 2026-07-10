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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Set;

/** Factory for the bounded GreptimeDB SQL table source. */
public final class GreptimeDynamicTableSourceFactory implements DynamicTableSourceFactory {

    /** Creates a GreptimeDB dynamic table source factory. */
    public GreptimeDynamicTableSourceFactory() {
    }

    @Override
    public String factoryIdentifier() {
        return GreptimeSourceOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return GreptimeSourceOptions.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return GreptimeSourceOptions.optionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return GreptimeSourceOptions.forwardOptions();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        String deferredFailure = inspectJdbcUrl(
                context.getCatalogTable().getOptions().get(GreptimeSourceOptions.JDBC_URL.key()));
        if (deferredFailure == null) {
            deferredFailure = inspectJdbcUrl(
                    context.getEnrichmentOptions().get(GreptimeSourceOptions.JDBC_URL.key()));
        }
        if (deferredFailure != null) {
            return GreptimeDynamicTableSource.withDeferredValidationFailure(
                    deferredFailure,
                    context.getPhysicalRowDataType());
        }

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        GreptimeResultSetRowDataConverter.validate((RowType) physicalRowDataType.getLogicalType());
        return new GreptimeDynamicTableSource(
                GreptimeSourceOptions.createQueryConfig(
                        options,
                        context.getObjectIdentifier().getObjectName()),
                physicalRowDataType);
    }

    private static String inspectJdbcUrl(String jdbcUrl) {
        return jdbcUrl == null ? null : GreptimeJdbcQueryConfig.deferredValidationFailure(jdbcUrl);
    }
}
