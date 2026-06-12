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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

final class GreptimeDynamicTableSinkFactory implements DynamicTableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return GreptimeConnectorOptions.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return GreptimeConnectorOptions.requiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return GreptimeConnectorOptions.optionalOptions();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return GreptimeConnectorOptions.forwardOptions();
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        ReadableConfig options = helper.getOptions();
        String table = context.getObjectIdentifier().getObjectName();
        return new GreptimeDynamicTableSink(GreptimeConnectorOptions.createSinkOptions(
                options,
                table,
                context.getCatalogTable().getResolvedSchema()));
    }
}
