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

import io.greptime.BulkStreamWriter;
import io.greptime.GreptimeDB;
import io.greptime.models.Table;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

final class GreptimeSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GreptimeSinkWriter.class);

    private final GreptimeDB greptimeDb;
    private final BulkStreamWriter writer;
    private final GreptimeRecordSerializer<T> recordSerializer;
    private final int batchSize;
    private final Queue<CompletableFuture<Integer>> pendingWrites;

    private Table.TableBufferRoot buffer;
    private int accumulatedRows;

    GreptimeSinkWriter(
            GreptimeDB greptimeDb,
            BulkStreamWriter writer,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize) {
        this.greptimeDb = greptimeDb;
        this.writer = writer;
        this.recordSerializer = recordSerializer;
        this.batchSize = batchSize;
        this.buffer = writer.tableBufferRoot(batchSize);
        this.accumulatedRows = 0;
        this.pendingWrites = new ArrayDeque<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(T element, Context context) {
        Object[] row = recordSerializer.serialize(element);
        buffer.addRow(row);
        accumulatedRows++;

        if (accumulatedRows >= batchSize) {
            submitBuffer();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(boolean endOfInput) {
        submitBuffer();
        waitForPendingWrites();
    }

    void submitBuffer() {
        if (accumulatedRows == 0) {
            return;
        }

        buffer.complete();

        long start = System.currentTimeMillis();
        try {
            CompletableFuture<Integer> future = writer.writeNext().whenComplete((rows, e) -> {
                if (e != null) {
                    LOGGER.error("Failed to write batch", e);
                } else if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Inserted {} rows, time cost: {} millis", rows, System.currentTimeMillis() - start);
                }
            });
            pendingWrites.add(future);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        buffer = writer.tableBufferRoot(batchSize);
        accumulatedRows = 0;
    }

    void waitForPendingWrites() {
        CompletableFuture<Integer> future;
        while ((future = pendingWrites.poll()) != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        try {
            flush(false);
            writer.completed();
            writer.close();
        } finally {
            greptimeDb.shutdownGracefully();
        }
    }
}
