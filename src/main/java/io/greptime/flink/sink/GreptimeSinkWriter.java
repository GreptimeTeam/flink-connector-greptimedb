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
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

final class GreptimeSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GreptimeSinkWriter.class);

    private final BulkStreamWriter writer;
    private final GreptimeRecordSerializer<T> recordSerializer;
    private final Runnable shutdownGreptimeDb;
    private final int batchSize;
    private final Queue<CompletableFuture<Integer>> pendingWrites;

    private Table.TableBufferRoot buffer;
    private int accumulatedRows;
    private final AtomicReference<Throwable> asyncWriteFailure;

    GreptimeSinkWriter(
            GreptimeDB greptimeDb,
            BulkStreamWriter writer,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize) {
        this(writer, recordSerializer, batchSize, greptimeDb::shutdownGracefully);
    }

    GreptimeSinkWriter(
            BulkStreamWriter writer,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize,
            Runnable shutdownGreptimeDb) {
        this.writer = writer;
        this.recordSerializer = recordSerializer;
        this.shutdownGreptimeDb = shutdownGreptimeDb;
        this.batchSize = batchSize;
        this.buffer = writer.tableBufferRoot(batchSize);
        this.accumulatedRows = 0;
        this.pendingWrites = new ArrayDeque<>();
        this.asyncWriteFailure = new AtomicReference<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(T element, Context context) {
        checkAsyncWriteFailure();
        pruneCompletedPendingWrites();

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
        checkAsyncWriteFailure();
        submitBuffer();
        waitForPendingWrites();
    }

    void submitBuffer() {
        checkAsyncWriteFailure();
        pruneCompletedPendingWrites();

        if (accumulatedRows == 0) {
            return;
        }

        buffer.complete();

        long start = System.currentTimeMillis();
        CompletableFuture<Integer> future;
        try {
            future = writer.writeNext().whenComplete((rows, e) -> {
                if (e != null) {
                    recordAsyncWriteFailure(e);
                    LOGGER.error("Failed to write batch", e);
                } else if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Inserted {} rows, time cost: {} millis", rows, System.currentTimeMillis() - start);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        pendingWrites.add(future);
        buffer = writer.tableBufferRoot(batchSize);
        accumulatedRows = 0;
        pruneCompletedPendingWrites();
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
                Throwable failure = unwrapAsyncWriteFailure(e);
                recordAsyncWriteFailure(failure);
                throw asyncWriteFailureException(failure);
            }
        }

        checkAsyncWriteFailure();
    }

    void pruneCompletedPendingWrites() {
        Iterator<CompletableFuture<Integer>> iterator = pendingWrites.iterator();
        while (iterator.hasNext()) {
            CompletableFuture<Integer> future = iterator.next();
            if (!future.isDone()) {
                continue;
            }

            try {
                future.get();
                iterator.remove();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                iterator.remove();
                Throwable failure = unwrapAsyncWriteFailure(e);
                recordAsyncWriteFailure(failure);
                throw asyncWriteFailureException(failure);
            }
        }
    }

    int pendingWritesSize() {
        return pendingWrites.size();
    }

    private void recordAsyncWriteFailure(Throwable failure) {
        asyncWriteFailure.compareAndSet(null, unwrapAsyncWriteFailure(failure));
    }

    private void checkAsyncWriteFailure() {
        Throwable failure = asyncWriteFailure.get();
        if (failure != null) {
            throw asyncWriteFailureException(failure);
        }
    }

    private RuntimeException asyncWriteFailureException(Throwable failure) {
        return new RuntimeException("Async write to GreptimeDB failed", failure);
    }

    private Throwable unwrapAsyncWriteFailure(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        try (BulkStreamWriter ignored = writer) {
            flush(false);
            writer.completed();
        } finally {
            shutdownGreptimeDb.run();
        }
    }
}
