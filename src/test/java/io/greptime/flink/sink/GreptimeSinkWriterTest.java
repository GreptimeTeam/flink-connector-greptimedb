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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.greptime.flink.sink;

import io.greptime.BulkStreamWriter;
import io.greptime.models.Table;
import io.greptime.v1.Database;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GreptimeSinkWriterTest {

    private static final long DEFAULT_TIMEOUT_MS = 10_000;

    @Test
    void shouldFailSubsequentWriteAfterAsyncWriteFails() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> firstWrite = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("write failed");
        bulkWriter.enqueueWrite(firstWrite);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 1);

        sinkWriter.write("first", sinkContext());
        firstWrite.completeExceptionally(failure);

        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                () -> sinkWriter.write("second", sinkContext()));
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
    }

    @Test
    void shouldFailFlushWhenPendingAsyncWriteFails() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        bulkWriter.enqueueWrite(failedFuture(failure));
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 2);

        sinkWriter.write("first", sinkContext());

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
    }

    @Test
    void shouldCloseBulkWriterWhenCloseTimeFlushFails() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        bulkWriter.enqueueWrite(failedFuture(failure));
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 2, () -> shutdownCalled.set(true));

        sinkWriter.write("first", sinkContext());

        RuntimeException thrown = assertThrows(RuntimeException.class, sinkWriter::close);
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertFalse(bulkWriter.completed);
        assertTrue(bulkWriter.closed);
        assertTrue(shutdownCalled.get());
    }

    @Test
    void shouldReportUnobservedAsyncWriteFailureOnCloseAndCleanup() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> firstWrite = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("write failed");
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        bulkWriter.enqueueWrite(firstWrite);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 1, () -> shutdownCalled.set(true));

        sinkWriter.write("first", sinkContext());
        firstWrite.completeExceptionally(failure);

        RuntimeException thrown = assertThrows(RuntimeException.class, sinkWriter::close);
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertFalse(bulkWriter.completed);
        assertTrue(bulkWriter.closed);
        assertTrue(shutdownCalled.get());
    }

    @Test
    void shouldCompleteCurrentStreamOnCheckpointFlushAndOpenNextStream() {
        RecordingBulkStreamWriterFactory writerFactory = new RecordingBulkStreamWriterFactory();
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(writerFactory, 2);

        sinkWriter.write("first", sinkContext());
        sinkWriter.flush(false);
        sinkWriter.write("second", sinkContext());
        sinkWriter.flush(false);

        assertEquals(2, writerFactory.writers.size());
        FakeBulkStreamWriter firstWriter = writerFactory.writers.get(0);
        FakeBulkStreamWriter secondWriter = writerFactory.writers.get(1);
        assertEquals(List.of(1), firstWriter.writeRows);
        assertEquals(List.of(1), secondWriter.writeRows);
        assertTrue(firstWriter.completed);
        assertTrue(secondWriter.completed);
        assertTrue(firstWriter.closed);
        assertTrue(secondWriter.closed);
    }

    @Test
    void shouldCompletePreviouslySubmittedStreamOnCheckpointFlush() {
        RecordingBulkStreamWriterFactory writerFactory = new RecordingBulkStreamWriterFactory();
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(writerFactory, 1);

        sinkWriter.write("first", sinkContext());
        sinkWriter.flush(false);

        assertEquals(1, writerFactory.writers.size());
        FakeBulkStreamWriter writer = writerFactory.writers.get(0);
        assertEquals(List.of(1), writer.writeRows);
        assertTrue(writer.completed);
        assertTrue(writer.closed);
    }

    @Test
    void shouldCompleteStreamOnEndOfInputAndRejectFurtherWrites() {
        RecordingBulkStreamWriterFactory writerFactory = new RecordingBulkStreamWriterFactory();
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(writerFactory, 2);

        sinkWriter.write("first", sinkContext());
        sinkWriter.flush(true);

        FakeBulkStreamWriter writer = writerFactory.writers.get(0);
        assertEquals(List.of(1), writer.writeRows);
        assertTrue(writer.completed);
        assertTrue(writer.closed);
        IllegalStateException thrown =
                assertThrows(IllegalStateException.class, () -> sinkWriter.write("second", sinkContext()));
        assertEquals("GreptimeDB sink writer is not open", thrown.getMessage());
    }

    @Test
    void shouldBlockConcurrentWriteWhileFlushCompletesStream() throws Exception {
        RecordingBulkStreamWriterFactory writerFactory = new RecordingBulkStreamWriterFactory();
        AtomicBoolean observeSerializer = new AtomicBoolean();
        CountDownLatch serializerEntered = new CountDownLatch(1);
        GreptimeSinkWriter<String> sinkWriter = new GreptimeSinkWriter<>(
                writerFactory,
                value -> {
                    if (observeSerializer.get()) {
                        serializerEntered.countDown();
                    }
                    return new Object[] { value };
                },
                2,
                DEFAULT_TIMEOUT_MS,
                () -> {
                });
        CountDownLatch completedStarted = new CountDownLatch(1);
        CountDownLatch allowCompleted = new CountDownLatch(1);
        AtomicBoolean writeFinished = new AtomicBoolean();
        AtomicReference<Throwable> flushFailure = new AtomicReference<>();
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        CountDownLatch writeStarted = new CountDownLatch(1);

        sinkWriter.write("first", sinkContext());
        writerFactory.writers.get(0).blockCompleted(completedStarted, allowCompleted);

        Thread flushThread = new Thread(() -> {
            try {
                sinkWriter.flush(false);
            } catch (Throwable t) {
                flushFailure.set(t);
            }
        }, "greptime-sink-flush-test");
        flushThread.start();
        assertTrue(completedStarted.await(5, TimeUnit.SECONDS));

        observeSerializer.set(true);
        Thread writeThread = new Thread(() -> {
            try {
                writeStarted.countDown();
                sinkWriter.write("second", sinkContext());
                writeFinished.set(true);
            } catch (Throwable t) {
                writeFailure.set(t);
            }
        }, "greptime-sink-write-test");
        writeThread.start();

        try {
            assertTrue(writeStarted.await(5, TimeUnit.SECONDS));
            awaitBlocked(writeThread);
            assertEquals(1, serializerEntered.getCount());
            assertFalse(writeFinished.get());
            assertEquals(1, writerFactory.writers.size());
        } finally {
            allowCompleted.countDown();
        }

        flushThread.join(5_000);
        writeThread.join(5_000);

        assertFalse(flushThread.isAlive());
        assertFalse(writeThread.isAlive());
        if (flushFailure.get() != null) {
            throw new AssertionError("Flush failed", flushFailure.get());
        }
        if (writeFailure.get() != null) {
            throw new AssertionError("Write failed", writeFailure.get());
        }
        assertTrue(serializerEntered.await(5, TimeUnit.SECONDS));

        sinkWriter.flush(false);

        assertEquals(2, writerFactory.writers.size());
        FakeBulkStreamWriter firstWriter = writerFactory.writers.get(0);
        FakeBulkStreamWriter secondWriter = writerFactory.writers.get(1);
        assertEquals(List.of(1), firstWriter.writeRows);
        assertEquals(List.of(1), secondWriter.writeRows);
        assertTrue(firstWriter.completed);
        assertTrue(secondWriter.completed);
    }

    @Test
    void shouldCompleteStreamAndShutdownClientOnClose() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 2, () -> shutdownCalled.set(true));

        sinkWriter.write("first", sinkContext());
        sinkWriter.close();

        assertEquals(List.of(1), bulkWriter.writeRows);
        assertTrue(bulkWriter.completed);
        assertTrue(bulkWriter.closed);
        assertTrue(shutdownCalled.get());
    }

    @Test
    void shouldCleanupCloseAfterFlushFailure() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        bulkWriter.enqueueWrite(failedFuture(failure));
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 2, () -> shutdownCalled.set(true));

        sinkWriter.write("first", sinkContext());
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        sinkWriter.close();

        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertFalse(bulkWriter.completed);
        assertTrue(bulkWriter.closed);
        assertTrue(shutdownCalled.get());
    }

    @Test
    void shouldCleanupCloseAfterSynchronousFlushFailure() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        bulkWriter.failWriteNext(failure);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 2, () -> shutdownCalled.set(true));

        sinkWriter.write("first", sinkContext());
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        sinkWriter.close();

        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertFalse(bulkWriter.completed);
        assertTrue(bulkWriter.closed);
        assertTrue(shutdownCalled.get());
    }

    @Test
    void shouldPruneCompletedPendingWritesOnWrite() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> firstWrite = new CompletableFuture<>();
        bulkWriter.enqueueWrite(firstWrite);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriter(bulkWriter, 1);

        sinkWriter.write("first", sinkContext());
        assertEquals(1, sinkWriter.pendingWritesSize());

        firstWrite.complete(1);

        sinkWriter.write("second", sinkContext());
        assertEquals(0, sinkWriter.pendingWritesSize());
    }

    @Test
    void shouldRecordSuccessfulFlushMetrics() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 2);

        sinkWriter.write("first", sinkContext());
        assertEquals(1, sinkWriter.metrics().bufferedRows());
        assertEquals(0, sinkWriter.metrics().pendingWrites());

        sinkWriter.flush(false);

        assertEquals(0, sinkWriter.metrics().bufferedRows());
        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(0, sinkWriter.metrics().recordsSendErrors());
        assertEquals(1, sinkWriter.metrics().flushTotal());
        assertEquals(1, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(0, sinkWriter.metrics().flushFailureTotal());
        assertEquals(1, sinkWriter.metrics().flushRowsTotal());
        assertEquals(1, sinkWriter.metrics().lastFlushRows());
        assertEquals(0, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    @Test
    void shouldReportOnlyActivePendingWritesMetric() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> firstWrite = new CompletableFuture<>();
        bulkWriter.enqueueWrite(firstWrite);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 1);

        sinkWriter.write("first", sinkContext());
        assertEquals(1, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.metrics().recordsSend());

        firstWrite.complete(1);
        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.pendingWritesSize());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(0, sinkWriter.metrics().flushTotal());

        sinkWriter.write("second", sinkContext());
        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(0, sinkWriter.pendingWritesSize());
    }

    @Test
    void shouldRecordFailedFlushMetricsOnce() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        bulkWriter.enqueueWrite(failedFuture(failure));
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 2);

        sinkWriter.write("first", sinkContext());

        assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(1, sinkWriter.metrics().recordsSendErrors());
        assertEquals(1, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(1, sinkWriter.metrics().flushFailureTotal());
        assertEquals(0, sinkWriter.metrics().flushRowsTotal());
        assertEquals(1, sinkWriter.metrics().lastFlushRows());
        assertEquals(1, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    @Test
    void shouldRecordSubmittedMetricsOnSynchronousFlushFailure() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        RuntimeException failure = new RuntimeException("write failed");
        bulkWriter.failWriteNext(failure);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 2);

        sinkWriter.write("first", sinkContext());

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(1, sinkWriter.metrics().recordsSendErrors());
        assertEquals(1, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(1, sinkWriter.metrics().flushFailureTotal());
        assertEquals(0, sinkWriter.metrics().flushRowsTotal());
        assertEquals(1, sinkWriter.metrics().lastFlushRows());
        assertEquals(1, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    @Test
    void shouldDrainFlushMetricsAfterWritesCompleteConcurrently() throws Exception {
        int writeCount = 128;
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        List<CompletableFuture<Integer>> writes = new ArrayList<>(writeCount);
        for (int i = 0; i < writeCount; i++) {
            CompletableFuture<Integer> write = new CompletableFuture<>();
            writes.add(write);
            bulkWriter.enqueueWrite(write);
        }
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 1);

        for (int i = 0; i < writeCount; i++) {
            sinkWriter.write("record-" + i, sinkContext());
        }
        assertEquals(writeCount, sinkWriter.metrics().pendingWrites());
        assertEquals(writeCount, sinkWriter.metrics().recordsSend());
        assertEquals(0, sinkWriter.metrics().flushTotal());

        CountDownLatch startCompletions = new CountDownLatch(1);
        List<Thread> completionThreads = new ArrayList<>(writeCount);
        for (int i = 0; i < writeCount; i++) {
            CompletableFuture<Integer> write = writes.get(i);
            Thread thread = new Thread(() -> {
                try {
                    startCompletions.await();
                    write.complete(1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }, "greptime-sink-metrics-completion-" + i);
            completionThreads.add(thread);
            thread.start();
        }

        startCompletions.countDown();
        for (Thread thread : completionThreads) {
            thread.join(TimeUnit.SECONDS.toMillis(5));
            assertFalse(thread.isAlive());
        }

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(writeCount, sinkWriter.metrics().recordsSend());
        assertEquals(writeCount, sinkWriter.pendingWritesSize());
        assertEquals(0, sinkWriter.metrics().recordsSendErrors());
        assertEquals(0, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(0, sinkWriter.metrics().flushFailureTotal());
        assertEquals(0, sinkWriter.metrics().flushRowsTotal());
        assertEquals(0, sinkWriter.metrics().asyncWriteFailureTotal());

        sinkWriter.flush(false);

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(0, sinkWriter.pendingWritesSize());
        assertEquals(writeCount, sinkWriter.metrics().recordsSend());
        assertEquals(0, sinkWriter.metrics().recordsSendErrors());
        assertEquals(writeCount, sinkWriter.metrics().flushTotal());
        assertEquals(writeCount, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(0, sinkWriter.metrics().flushFailureTotal());
        assertEquals(writeCount, sinkWriter.metrics().flushRowsTotal());
        assertEquals(1, sinkWriter.metrics().lastFlushRows());
        assertEquals(0, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    @Test
    void shouldDrainFailureMetricsAfterWriteCompletesExceptionally() throws Exception {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> write = new CompletableFuture<>();
        RuntimeException failure = new RuntimeException("write failed");
        bulkWriter.enqueueWrite(write);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 1);

        sinkWriter.write("first", sinkContext());
        assertEquals(1, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.metrics().recordsSend());

        Thread completionThread = new Thread(
                () -> write.completeExceptionally(failure),
                "greptime-sink-metrics-failed-completion");
        completionThread.start();
        completionThread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(completionThread.isAlive());

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.pendingWritesSize());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(0, sinkWriter.metrics().recordsSendErrors());
        assertEquals(0, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushFailureTotal());
        assertEquals(0, sinkWriter.metrics().asyncWriteFailureTotal());

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(0, sinkWriter.pendingWritesSize());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(1, sinkWriter.metrics().recordsSendErrors());
        assertEquals(1, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(1, sinkWriter.metrics().flushFailureTotal());
        assertEquals(1, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    @Test
    void shouldRecordCancelledWriteAsFailureWhenDrained() {
        FakeBulkStreamWriter bulkWriter = new FakeBulkStreamWriter();
        CompletableFuture<Integer> write = new CompletableFuture<>();
        bulkWriter.enqueueWrite(write);
        GreptimeSinkWriter<String> sinkWriter = newSinkWriterWithMetrics(bulkWriter, 1);

        sinkWriter.write("first", sinkContext());
        assertEquals(1, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.metrics().recordsSend());

        write.cancel(false);

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(1, sinkWriter.pendingWritesSize());
        assertEquals(0, sinkWriter.metrics().recordsSendErrors());
        assertEquals(0, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushFailureTotal());
        assertEquals(0, sinkWriter.metrics().asyncWriteFailureTotal());

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> sinkWriter.flush(false));
        assertEquals("Async write to GreptimeDB failed", thrown.getMessage());
        assertTrue(thrown.getCause() instanceof CancellationException);

        assertEquals(0, sinkWriter.metrics().pendingWrites());
        assertEquals(0, sinkWriter.pendingWritesSize());
        assertEquals(1, sinkWriter.metrics().recordsSend());
        assertEquals(1, sinkWriter.metrics().recordsSendErrors());
        assertEquals(1, sinkWriter.metrics().flushTotal());
        assertEquals(0, sinkWriter.metrics().flushSuccessTotal());
        assertEquals(1, sinkWriter.metrics().flushFailureTotal());
        assertEquals(1, sinkWriter.metrics().asyncWriteFailureTotal());
    }

    private static GreptimeSinkWriter<String> newSinkWriter(FakeBulkStreamWriter bulkWriter, int batchSize) {
        return newSinkWriter(bulkWriter, batchSize, () -> {
        });
    }

    private static GreptimeSinkWriter<String> newSinkWriter(
            RecordingBulkStreamWriterFactory writerFactory,
            int batchSize) {
        return new GreptimeSinkWriter<>(
                writerFactory,
                value -> new Object[] { value },
                batchSize,
                DEFAULT_TIMEOUT_MS,
                () -> {
                });
    }

    private static GreptimeSinkWriter<String> newSinkWriter(
            FakeBulkStreamWriter bulkWriter,
            int batchSize,
            Runnable shutdownGreptimeDb) {
        return new GreptimeSinkWriter<>(
                new SingleBulkStreamWriterFactory(bulkWriter),
                value -> new Object[] { value },
                batchSize,
                DEFAULT_TIMEOUT_MS,
                shutdownGreptimeDb);
    }

    private static GreptimeSinkWriter<String> newSinkWriterWithMetrics(
            FakeBulkStreamWriter bulkWriter,
            int batchSize) {
        return new GreptimeSinkWriter<>(
                new SingleBulkStreamWriterFactory(bulkWriter),
                value -> new Object[] { value },
                batchSize,
                DEFAULT_TIMEOUT_MS,
                () -> {
                },
                sinkWriterMetricGroup());
    }

    private static SinkWriterMetricGroup sinkWriterMetricGroup() {
        return UnregisteredMetricsGroup.createSinkWriterMetricGroup();
    }

    private static SinkWriter.Context sinkContext() {
        return new SinkWriter.Context() {
            @Override
            public long currentWatermark() {
                return 0;
            }

            @Override
            public Long timestamp() {
                return null;
            }
        };
    }

    private static CompletableFuture<Integer> failedFuture(Throwable failure) {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        future.completeExceptionally(failure);
        return future;
    }

    private static void awaitBlocked(Thread thread) throws InterruptedException {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive()
                && thread.getState() != Thread.State.BLOCKED
                && System.nanoTime() < deadlineNanos) {
            Thread.sleep(10);
        }
        assertEquals(Thread.State.BLOCKED, thread.getState());
    }

    private static final class RecordingBulkStreamWriterFactory implements BulkWriteClient.BulkStreamWriterFactory {

        private final List<FakeBulkStreamWriter> writers = new ArrayList<>();

        @Override
        public BulkStreamWriter create() {
            FakeBulkStreamWriter writer = new FakeBulkStreamWriter();
            writers.add(writer);
            return writer;
        }
    }

    private static final class SingleBulkStreamWriterFactory implements BulkWriteClient.BulkStreamWriterFactory {

        private final FakeBulkStreamWriter writer;

        private SingleBulkStreamWriterFactory(FakeBulkStreamWriter writer) {
            this.writer = writer;
        }

        @Override
        public BulkStreamWriter create() {
            return writer;
        }
    }

    private static final class FakeBulkStreamWriter implements BulkStreamWriter {

        private final Queue<CompletableFuture<Integer>> writes = new ArrayDeque<>();
        private final List<Integer> writeRows = new ArrayList<>();
        private FakeTableBufferRoot currentTable;
        private RuntimeException writeNextFailure;
        private CountDownLatch completedStarted;
        private CountDownLatch allowCompleted;
        private boolean completed;
        private boolean closed;

        void enqueueWrite(CompletableFuture<Integer> future) {
            writes.add(future);
        }

        void failWriteNext(RuntimeException failure) {
            this.writeNextFailure = failure;
        }

        void blockCompleted(CountDownLatch completedStarted, CountDownLatch allowCompleted) {
            this.completedStarted = completedStarted;
            this.allowCompleted = allowCompleted;
        }

        @Override
        public Table.TableBufferRoot tableBufferRoot(int maxRows) {
            currentTable = new FakeTableBufferRoot();
            return currentTable;
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            if (writeNextFailure != null) {
                throw writeNextFailure;
            }
            writeRows.add(currentTable.rowCount());
            CompletableFuture<Integer> future = writes.poll();
            if (future == null) {
                return CompletableFuture.completedFuture(currentTable.rowCount());
            }
            return future;
        }

        @Override
        public void completed() {
            if (completedStarted != null) {
                completedStarted.countDown();
            }
            if (allowCompleted != null) {
                try {
                    allowCompleted.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            completed = true;
            closed = true;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static final class FakeTableBufferRoot implements Table.TableBufferRoot {

        private int rowCount;
        private boolean completed;

        @Override
        public String tableName() {
            return "metrics";
        }

        @Override
        public int rowCount() {
            return rowCount;
        }

        @Override
        public int columnCount() {
            return 1;
        }

        @Override
        public long bytesUsed() {
            return rowCount;
        }

        @Override
        public Table addRow(Object... values) {
            rowCount++;
            return this;
        }

        @Override
        public Table subRange(int from, int to) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Table complete() {
            completed = true;
            return this;
        }

        @Override
        public boolean isCompleted() {
            return completed;
        }

        @Override
        public Database.RowInsertRequest intoRowInsertRequest() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Database.RowDeleteRequest intoRowDeleteRequest() {
            throw new UnsupportedOperationException();
        }
    }
}
