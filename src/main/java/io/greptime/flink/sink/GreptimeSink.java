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

import io.greptime.BulkWrite;
import io.greptime.GreptimeDB;
import io.greptime.models.AuthInfo;
import io.greptime.models.TableSchema;
import io.greptime.options.GreptimeOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Flink Sink V2 implementation that writes records into GreptimeDB through the
 * ingester client.
 *
 * @param <T> the Flink record type consumed by this sink
 */
public final class GreptimeSink<T> implements Sink<T> {

    private final List<String> endpoints;
    private final String database;
    private final GreptimeTableSchema tableSchema;
    private final GreptimeRecordSerializer<T> recordSerializer;
    private final int batchSize;
    private final long allocatorInitReservation;
    private final long allocatorMaxAllocation;
    private final long timeoutMsPerMessage;
    private final int maxRequestsInFlight;
    private final String username;
    private final String password;

    private GreptimeSink(Builder<T> builder) {
        this.endpoints = builder.endpoints;
        this.database = builder.database;
        this.tableSchema = GreptimeTableSchema.from(builder.tableSchema);
        this.recordSerializer = builder.recordSerializer;
        this.batchSize = builder.batchSize;
        this.allocatorInitReservation = builder.allocatorInitReservation;
        this.allocatorMaxAllocation = builder.allocatorMaxAllocation;
        this.timeoutMsPerMessage = builder.timeoutMsPerMessage;
        this.maxRequestsInFlight = builder.maxRequestsInFlight;
        this.username = builder.username;
        this.password = builder.password;
    }

    /**
     * Creates a builder for configuring a GreptimeDB sink.
     *
     * @param <T> the Flink record type consumed by the sink
     * @return a new GreptimeDB sink builder
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) throws IOException {
        GreptimeOptions.Builder optionsBuilder = GreptimeOptions.newBuilder(
                endpoints.toArray(new String[0]),
                database);
        if (username != null) {
            optionsBuilder.authInfo(new AuthInfo(username, password));
        }

        GreptimeDB greptimeDb = GreptimeDB.create(optionsBuilder.build());
        BulkWrite.Config bulkWriteConfig = BulkWrite.Config.newBuilder()
                .allocatorInitReservation(allocatorInitReservation)
                .allocatorMaxAllocation(allocatorMaxAllocation)
                .timeoutMsPerMessage(timeoutMsPerMessage)
                .maxRequestsInFlight(maxRequestsInFlight)
                .build();

        return new GreptimeSinkWriter<>(
                greptimeDb,
                () -> greptimeDb.bulkStreamWriter(tableSchema.toTableSchema(), bulkWriteConfig),
                recordSerializer,
                batchSize);
    }

    /**
     * Builder for {@link GreptimeSink}.
     *
     * @param <T> the Flink record type consumed by the sink
     */
    public static final class Builder<T> {

        private static final String DEFAULT_DATABASE = "public";
        private static final int DEFAULT_BATCH_SIZE = 1_000;

        private List<String> endpoints = List.of();
        private String database = DEFAULT_DATABASE;
        private TableSchema tableSchema;
        private GreptimeRecordSerializer<T> recordSerializer;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private long allocatorInitReservation = BulkWrite.DEFAULT_ALLOCATOR_INIT_RESERVATION;
        private long allocatorMaxAllocation = BulkWrite.DEFAULT_ALLOCATOR_MAX_ALLOCATION;
        private long timeoutMsPerMessage = BulkWrite.DEFAULT_TIMEOUT_MS_PER_MESSAGE;
        private int maxRequestsInFlight = BulkWrite.DEFAULT_MAX_REQUESTS_IN_FLIGHT;
        private String username;
        private String password;

        private Builder() {
        }

        /**
         * Configures the GreptimeDB ingester endpoint.
         *
         * @param endpoint the ingester endpoint, for example {@code 127.0.0.1:4001}
         * @return this builder
         */
        public Builder<T> endpoint(String endpoint) {
            return endpoints(List.of(Objects.requireNonNull(endpoint, "endpoint must not be null")));
        }

        /**
         * Configures the GreptimeDB ingester endpoints.
         *
         * @param endpoints the ingester endpoints, for example {@code 127.0.0.1:4001}
         * @return this builder
         */
        public Builder<T> endpoints(List<String> endpoints) {
            Objects.requireNonNull(endpoints, "endpoints must not be null");
            if (endpoints.isEmpty()) {
                throw new IllegalArgumentException("endpoints must not be empty");
            }
            for (String endpoint : endpoints) {
                if (StringUtils.isBlank(endpoint)) {
                    throw new IllegalArgumentException("endpoint must not be blank");
                }
            }
            this.endpoints = List.copyOf(endpoints);
            return this;
        }

        /**
         * Configures the GreptimeDB database name.
         *
         * @param database the target database name
         * @return this builder
         */
        public Builder<T> database(String database) {
            this.database = Objects.requireNonNull(database, "database must not be null");
            return this;
        }

        /**
         * Configures the GreptimeDB table schema used by the ingester writer.
         *
         * @param tableSchema the target table schema
         * @return this builder
         */
        public Builder<T> tableSchema(TableSchema tableSchema) {
            this.tableSchema = Objects.requireNonNull(tableSchema, "tableSchema must not be null");
            return this;
        }

        /**
         * Configures the serializer that maps Flink records to GreptimeDB rows.
         *
         * @param recordSerializer the record serializer
         * @return this builder
         */
        public Builder<T> recordSerializer(GreptimeRecordSerializer<T> recordSerializer) {
            this.recordSerializer = Objects.requireNonNull(recordSerializer, "recordSerializer must not be null");
            return this;
        }

        /**
         * Configures the maximum number of rows buffered before a write is flushed.
         *
         * @param batchSize the positive batch size
         * @return this builder
         */
        public Builder<T> batchSize(int batchSize) {
            if (batchSize <= 0) {
                throw new IllegalArgumentException("batchSize must be greater than zero");
            }
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Configures the initial Arrow allocator reservation used by the bulk writer.
         *
         * @param allocatorInitReservation the positive initial allocator reservation in
         *                                 bytes
         * @return this builder
         */
        public Builder<T> allocatorInitReservation(long allocatorInitReservation) {
            if (allocatorInitReservation <= 0) {
                throw new IllegalArgumentException("allocatorInitReservation must be greater than zero");
            }
            this.allocatorInitReservation = allocatorInitReservation;
            return this;
        }

        /**
         * Configures the maximum Arrow allocator allocation used by the bulk writer.
         *
         * @param allocatorMaxAllocation the positive maximum allocator allocation in
         *                               bytes
         * @return this builder
         */
        public Builder<T> allocatorMaxAllocation(long allocatorMaxAllocation) {
            if (allocatorMaxAllocation <= 0) {
                throw new IllegalArgumentException("allocatorMaxAllocation must be greater than zero");
            }
            this.allocatorMaxAllocation = allocatorMaxAllocation;
            return this;
        }

        /**
         * Configures the per-message write timeout used by the bulk writer.
         *
         * @param timeoutMsPerMessage the positive timeout in milliseconds
         * @return this builder
         */
        public Builder<T> timeoutMsPerMessage(long timeoutMsPerMessage) {
            if (timeoutMsPerMessage <= 0) {
                throw new IllegalArgumentException("timeoutMsPerMessage must be greater than zero");
            }
            this.timeoutMsPerMessage = timeoutMsPerMessage;
            return this;
        }

        /**
         * Configures the maximum number of concurrent bulk write requests.
         *
         * @param maxRequestsInFlight the positive maximum number of in-flight requests
         * @return this builder
         */
        public Builder<T> maxRequestsInFlight(int maxRequestsInFlight) {
            if (maxRequestsInFlight <= 0) {
                throw new IllegalArgumentException("maxRequestsInFlight must be greater than zero");
            }
            this.maxRequestsInFlight = maxRequestsInFlight;
            return this;
        }

        /**
         * Configures plaintext authentication for the GreptimeDB ingester connection.
         *
         * @param username the GreptimeDB username
         * @param password the GreptimeDB password
         * @return this builder
         */
        public Builder<T> plainTextAuth(String username, String password) {
            Objects.requireNonNull(username, "username must not be null");
            Objects.requireNonNull(password, "password must not be null");
            if (StringUtils.isBlank(username)) {
                throw new IllegalArgumentException("username must not be blank");
            }
            if (StringUtils.isBlank(password)) {
                throw new IllegalArgumentException("password must not be blank");
            }
            this.username = username;
            this.password = password;
            return this;
        }

        /**
         * Builds a configured {@link GreptimeSink}.
         *
         * @return the configured GreptimeDB sink
         */
        public GreptimeSink<T> build() {
            if (endpoints.isEmpty()) {
                throw new IllegalStateException("endpoint must be configured");
            }
            if (StringUtils.isBlank(database)) {
                throw new IllegalStateException("database must be configured");
            }
            Objects.requireNonNull(tableSchema, "tableSchema must be configured");
            Objects.requireNonNull(recordSerializer, "recordSerializer must be configured");

            if (allocatorMaxAllocation < allocatorInitReservation) {
                throw new IllegalStateException(
                        "allocatorMaxAllocation must be greater than or equal to allocatorInitReservation");
            }

            return new GreptimeSink<>(this);
        }
    }
}
