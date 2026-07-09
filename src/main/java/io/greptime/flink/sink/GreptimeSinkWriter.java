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
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class GreptimeSinkWriter<T> implements SinkWriter<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GreptimeSinkWriter.class);

    private final BulkStreamWriterFactory writerFactory;
    private final GreptimeRecordSerializer<T> recordSerializer;
    private final Runnable shutdownGreptimeDb;
    private final int batchSize;
    private final Queue<PendingWrite> pendingWrites;
    private final AtomicInteger activePendingWrites;
    private final GreptimeSinkWriterMetrics metrics;

    private BulkStreamWriter writer;
    private Table.TableBufferRoot buffer;
    private volatile int accumulatedRows;
    private boolean streamHasData;
    private boolean completed;
    private boolean closed;
    private boolean asyncWriteFailureObserved;
    private final AtomicReference<Throwable> asyncWriteFailure;

    GreptimeSinkWriter(
            GreptimeDB greptimeDb,
            BulkStreamWriterFactory writerFactory,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize) {
        this(greptimeDb, writerFactory, recordSerializer, batchSize, null);
    }

    GreptimeSinkWriter(
            GreptimeDB greptimeDb,
            BulkStreamWriterFactory writerFactory,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize,
            SinkWriterMetricGroup metricGroup) {
        this(writerFactory, recordSerializer, batchSize, greptimeDb::shutdownGracefully, metricGroup);
    }

    GreptimeSinkWriter(
            BulkStreamWriterFactory writerFactory,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize,
            Runnable shutdownGreptimeDb) {
        this(writerFactory, recordSerializer, batchSize, shutdownGreptimeDb, null);
    }

    GreptimeSinkWriter(
            BulkStreamWriterFactory writerFactory,
            GreptimeRecordSerializer<T> recordSerializer,
            int batchSize,
            Runnable shutdownGreptimeDb,
            SinkWriterMetricGroup metricGroup) {
        this.writerFactory = writerFactory;
        this.recordSerializer = recordSerializer;
        this.shutdownGreptimeDb = shutdownGreptimeDb;
        this.batchSize = batchSize;
        this.pendingWrites = new ArrayDeque<>();
        this.activePendingWrites = new AtomicInteger();
        this.asyncWriteFailure = new AtomicReference<>();
        this.writer = null;
        this.buffer = null;
        this.accumulatedRows = 0;
        this.streamHasData = false;
        this.completed = false;
        this.closed = false;
        this.asyncWriteFailureObserved = false;
        this.metrics =
                GreptimeSinkWriterMetrics.create(metricGroup, this::bufferedRowsSize, this::activePendingWritesSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void write(T element, Context context) {
        ensureOpenForWrite();
        checkAsyncWriteFailure();
        pruneCompletedPendingWrites();

        Object[] row = recordSerializer.serialize(element);
        ensureWriter();
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
    public synchronized void flush(boolean endOfInput) {
        if (completed || closed) {
            return;
        }
        checkAsyncWriteFailure();
        completeCurrentStream();
        if (endOfInput) {
            completed = true;
        }
    }

    private void ensureWriter() {
        if (writer == null) {
            writer = writerFactory.create();
        }
        if (buffer == null) {
            buffer = writer.tableBufferRoot(batchSize);
        }
    }

    private void completeCurrentStream() {
        submitBuffer();
        waitForPendingWrites();
        if (!streamHasData) {
            return;
        }

        try {
            writer.completed();
        } catch (Exception e) {
            recordAsyncWriteFailure(e);
            throw asyncWriteFailureException(e);
        }

        writer = null;
        buffer = null;
        accumulatedRows = 0;
        streamHasData = false;
    }

    private void submitBuffer() {
        checkAsyncWriteFailure();
        pruneCompletedPendingWrites();

        if (accumulatedRows == 0) {
            return;
        }

        int rows = accumulatedRows;
        buffer.complete();

        long startNanos = System.nanoTime();
        metrics.recordFlushSubmitted(rows);
        PendingWrite pendingWrite;
        try {
            CompletableFuture<Integer> future =
                    Objects.requireNonNull(writer.writeNext(), "write future must not be null");
            pendingWrite = new PendingWrite(future, rows, startNanos);
        } catch (Exception e) {
            metrics.recordFlushFailure(rows, elapsedMillis(startNanos));
            recordAsyncWriteFailure(e);
            throw asyncWriteFailureException(e);
        }

        pendingWrites.add(pendingWrite);
        activePendingWrites.incrementAndGet();
        pendingWrite.future().whenComplete((affectedRows, e) -> {
            if (pendingWrite.markCompleted()) {
                activePendingWrites.decrementAndGet();
            }
        });
        streamHasData = true;
        buffer = null;
        accumulatedRows = 0;
        pruneCompletedPendingWrites();
    }

    private void waitForPendingWrites() {
        PendingWrite pendingWrite;
        while ((pendingWrite = pendingWrites.poll()) != null) {
            recordCompletedPendingWrite(pendingWrite);
        }

        checkAsyncWriteFailure();
    }

    private void pruneCompletedPendingWrites() {
        Iterator<PendingWrite> iterator = pendingWrites.iterator();
        while (iterator.hasNext()) {
            PendingWrite pendingWrite = iterator.next();
            if (!pendingWrite.isDone()) {
                continue;
            }

            iterator.remove();
            recordCompletedPendingWrite(pendingWrite);
        }
    }

    private void recordCompletedPendingWrite(PendingWrite pendingWrite) {
        try {
            int affectedRows = pendingWrite.get();
            if (pendingWrite.markCompleted()) {
                activePendingWrites.decrementAndGet();
            }
            long durationMs = pendingWrite.durationMs();
            metrics.recordFlushSuccess(pendingWrite.rows(), durationMs);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Inserted {} rows, time cost: {} millis", affectedRows, durationMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable failure = unwrapAsyncWriteFailure(e);
            if (pendingWrite.markCompleted()) {
                activePendingWrites.decrementAndGet();
            }
            throw recordCompletedPendingWriteFailure(pendingWrite, failure);
        } catch (CancellationException e) {
            if (pendingWrite.markCompleted()) {
                activePendingWrites.decrementAndGet();
            }
            throw recordCompletedPendingWriteFailure(pendingWrite, e);
        }
    }

    private RuntimeException recordCompletedPendingWriteFailure(PendingWrite pendingWrite, Throwable failure) {
        metrics.recordFlushFailure(pendingWrite.rows(), pendingWrite.durationMs());
        recordAsyncWriteFailure(failure);
        LOGGER.error("Failed to write batch", failure);
        return asyncWriteFailureException(failure);
    }

    synchronized int pendingWritesSize() {
        return pendingWrites.size();
    }

    int activePendingWritesSize() {
        return activePendingWrites.get();
    }

    int bufferedRowsSize() {
        return accumulatedRows;
    }

    GreptimeSinkWriterMetrics metrics() {
        return metrics;
    }

    private void recordAsyncWriteFailure(Throwable failure) {
        if (asyncWriteFailure.compareAndSet(null, unwrapAsyncWriteFailure(failure))) {
            metrics.recordAsyncWriteFailure();
        }
    }

    private void checkAsyncWriteFailure() {
        Throwable failure = asyncWriteFailure.get();
        if (failure != null) {
            throw asyncWriteFailureException(failure);
        }
    }

    private RuntimeException asyncWriteFailureException(Throwable failure) {
        asyncWriteFailureObserved = true;
        return new RuntimeException("Async write to GreptimeDB failed", failure);
    }

    private Throwable unwrapAsyncWriteFailure(Throwable failure) {
        while ((failure instanceof CompletionException || failure instanceof ExecutionException)
                && failure.getCause() != null) {
            failure = failure.getCause();
        }
        return failure;
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private void ensureOpenForWrite() {
        if (completed || closed) {
            throw new IllegalStateException("GreptimeDB sink writer is not open");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void close() throws Exception {
        if (closed) {
            return;
        }

        Exception failure = null;
        try {
            Throwable asyncFailure = asyncWriteFailure.get();
            if (asyncFailure != null) {
                if (!asyncWriteFailureObserved) {
                    failure = asyncWriteFailureException(asyncFailure);
                }
            } else if (!completed) {
                try {
                    completeCurrentStream();
                } catch (Exception e) {
                    failure = e;
                }
            }
        } finally {
            Exception cleanupFailure = cleanupResources();
            closed = true;
            if (failure != null) {
                if (cleanupFailure != null) {
                    failure.addSuppressed(cleanupFailure);
                }
                throw failure;
            }
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
        }
    }

    private Exception cleanupResources() {
        Exception failure = null;
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                failure = e;
            } finally {
                writer = null;
                buffer = null;
            }
        }

        try {
            shutdownGreptimeDb.run();
        } catch (RuntimeException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        return failure;
    }

    @FunctionalInterface
    interface BulkStreamWriterFactory {
        BulkStreamWriter create();
    }

    private static final class PendingWrite {

        private final CompletableFuture<Integer> future;
        private final int rows;
        private final long startNanos;
        private final AtomicBoolean completed;
        private volatile long completionDurationMs = -1;

        private PendingWrite(CompletableFuture<Integer> future, int rows, long startNanos) {
            this.future = future;
            this.rows = rows;
            this.startNanos = startNanos;
            this.completed = new AtomicBoolean();
        }

        private boolean markCompleted() {
            if (!completed.compareAndSet(false, true)) {
                return false;
            }
            completionDurationMs = elapsedMillis(startNanos);
            return true;
        }

        private boolean isDone() {
            return future.isDone();
        }

        private int get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        private CompletableFuture<Integer> future() {
            return future;
        }

        private int rows() {
            return rows;
        }

        private long durationMs() {
            long durationMs = completionDurationMs;
            if (durationMs >= 0) {
                return durationMs;
            }
            return elapsedMillis(startNanos);
        }
    }
}
