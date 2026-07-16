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
import io.greptime.models.Table;
import io.greptime.v1.Database;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BulkWriteClientTest {

    private static final long TIMEOUT_MS = 100;

    @Test
    void shouldOpenStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        SingleWriterFactory factory = new SingleWriterFactory(writer);
        BulkWriteClient client = new BulkWriteClient(factory, () -> {
        });

        client.ensureStreamOpen(TIMEOUT_MS);

        assertNotNull(client.newTableBuffer(10));
        assertTrue(factory.createCalled);
    }

    @Test
    void shouldNotReopenStreamIfAlreadyOpen() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        SingleWriterFactory factory = new SingleWriterFactory(writer);
        BulkWriteClient client = new BulkWriteClient(factory, () -> {
        });

        client.ensureStreamOpen(TIMEOUT_MS);
        client.ensureStreamOpen(TIMEOUT_MS);

        assertEquals(1, factory.createCount);
    }

    @Test
    void shouldTimeoutOnStreamOpenAndNotSetWriter() {
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        BlockingFactory factory = new BlockingFactory();
        BulkWriteClient client = new BulkWriteClient(factory, () -> shutdownCalled.set(true));

        assertThrows(TimeoutException.class, () -> client.ensureStreamOpen(TIMEOUT_MS));
        assertTrue(factory.createAttempted);
        assertFalse(shutdownCalled.get());
    }

    @Test
    void shouldHandleInterruptOnStreamOpenAndPreserveInterruptStatus() throws Exception {
        BlockingFactory factory = new BlockingFactory();
        BulkWriteClient client = new BulkWriteClient(factory, () -> {
        });

        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread testThread = new Thread(() -> {
            started.countDown();
            try {
                client.ensureStreamOpen(10_000);
            } catch (Throwable e) {
                caught.set(e);
            }
        });
        testThread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        awaitWaiting(testThread);
        testThread.interrupt();
        testThread.join(5_000);

        assertTrue(caught.get() instanceof InterruptedException);
        assertTrue(testThread.isInterrupted() || caught.get() instanceof InterruptedException);
        assertTrue(factory.createAttempted);
    }

    @Test
    void shouldWriteNext() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        writer.allowWriteNext();
        CompletableFuture<Integer> future = client.writeNext(TIMEOUT_MS);

        assertNotNull(future);
        assertEquals(1, future.get(1, TimeUnit.SECONDS));
    }

    @Test
    void shouldTimeoutOnWriteNextAndCloseStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        assertThrows(TimeoutException.class, () -> {
            client.writeNext(TIMEOUT_MS);
        });
        assertTrue(writer.closed);
    }

    @Test
    void shouldHandleInterruptOnWriteNextAndCloseStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread testThread = new Thread(() -> {
            started.countDown();
            try {
                client.writeNext(10_000);
            } catch (Throwable e) {
                caught.set(e);
            }
        });
        testThread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        awaitWaiting(testThread);
        testThread.interrupt();
        testThread.join(5_000);

        assertTrue(caught.get() instanceof InterruptedException);
        assertTrue(writer.closed);
    }

    @Test
    void shouldPropagateSdkExceptionFromWriteNext() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        RuntimeException sdkError = new RuntimeException("SDK error");
        writer.failWriteNext(sdkError);
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        Exception thrown = assertThrows(Exception.class, () -> {
            client.writeNext(TIMEOUT_MS);
        });

        assertSame(sdkError, thrown);
    }

    @Test
    void shouldCompleted() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        writer.allowCompleted();
        client.completed(TIMEOUT_MS);

        assertTrue(writer.completedCalled);
        assertTrue(writer.closed);
    }

    @Test
    void shouldTimeoutOnCompletedAndCloseStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        assertThrows(TimeoutException.class, () -> {
            client.completed(TIMEOUT_MS);
        });
        assertTrue(writer.closed);
    }

    @Test
    void shouldHandleInterruptOnCompletedAndCloseStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Thread testThread = new Thread(() -> {
            started.countDown();
            try {
                client.completed(10_000);
            } catch (Throwable e) {
                caught.set(e);
            }
        });
        testThread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));
        awaitWaiting(testThread);
        testThread.interrupt();
        testThread.join(5_000);

        assertTrue(caught.get() instanceof InterruptedException);
        assertTrue(writer.closed);
    }

    @Test
    void shouldPropagateSdkExceptionFromCompleted() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        RuntimeException sdkError = new RuntimeException("SDK error");
        writer.failCompleted(sdkError);
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        Exception thrown = assertThrows(Exception.class, () -> {
            client.completed(TIMEOUT_MS);
        });

        assertSame(sdkError, thrown);
    }

    @Test
    void shouldCloseStream() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        client.closeStream();

        assertTrue(writer.closed);
    }

    @Test
    void shouldDoubleCloseStreamBeIdempotent() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        client.closeStream();
        client.closeStream();

        assertTrue(writer.closed);
    }

    @Test
    void shouldCloseSilentlyWhenNoStreamOpen() throws Exception {
        BulkWriteClient client = new BulkWriteClient(
                ControllableWriter::new,
                () -> {
                });
        client.closeStream();
    }

    @Test
    void shouldShutdownExecutorAndClient() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        AtomicBoolean clientShutdownCalled = new AtomicBoolean();
        BulkWriteClient client = new BulkWriteClient(
                new SingleWriterFactory(writer),
                () -> clientShutdownCalled.set(true));

        client.ensureStreamOpen(TIMEOUT_MS);
        client.shutdown();

        assertTrue(clientShutdownCalled.get());
    }

    @Test
    void shouldThrowWhenWriteNextOnClosedStream() {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        assertThrows(IllegalStateException.class, () -> client.writeNext(TIMEOUT_MS));
    }

    @Test
    void shouldThrowWhenCompletedOnClosedStream() {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        assertThrows(IllegalStateException.class, () -> client.completed(TIMEOUT_MS));
    }

    @Test
    void shouldReturnNewTableBuffer() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        Table.TableBufferRoot buffer = client.newTableBuffer(100);

        assertNotNull(buffer);
    }

    @Test
    void shouldNotCallShutdownClientOnTimeout() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        AtomicBoolean shutdownCalled = new AtomicBoolean();
        BulkWriteClient client = new BulkWriteClient(
                new SingleWriterFactory(writer),
                () -> shutdownCalled.set(true));

        client.ensureStreamOpen(TIMEOUT_MS);
        assertThrows(TimeoutException.class, () -> client.writeNext(TIMEOUT_MS));

        assertFalse(shutdownCalled.get());
    }

    @Test
    void shouldReleaseWriterAfterCompleted() throws Exception {
        ControllableWriter writer = new ControllableWriter();
        BulkWriteClient client = newClient(writer);

        client.ensureStreamOpen(TIMEOUT_MS);
        writer.allowCompleted();
        client.completed(TIMEOUT_MS);

        assertThrows(IllegalStateException.class, () -> client.writeNext(TIMEOUT_MS));
    }

    @Test
    void shouldBeAbleToReopenAfterCompleted() throws Exception {
        ControllableWriter firstWriter = new ControllableWriter();
        ControllableWriter secondWriter = new ControllableWriter();
        AtomicReference<BulkStreamWriter> nextWriter = new AtomicReference<>(firstWriter);
        BulkWriteClient client = new BulkWriteClient(nextWriter::get, () -> {
        });

        client.ensureStreamOpen(TIMEOUT_MS);
        firstWriter.allowCompleted();
        client.completed(TIMEOUT_MS);
        assertTrue(firstWriter.completedCalled);

        nextWriter.set(secondWriter);
        client.ensureStreamOpen(TIMEOUT_MS);
        secondWriter.allowWriteNext();
        CompletableFuture<Integer> future = client.writeNext(TIMEOUT_MS);
        assertEquals(1, future.get(1, TimeUnit.SECONDS));
    }

    private static BulkWriteClient newClient(ControllableWriter writer) {
        return new BulkWriteClient(new SingleWriterFactory(writer), () -> {
        });
    }

    private static void awaitWaiting(Thread thread) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive()
                && thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING
                && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertTrue(
                thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING,
                "Expected thread to be WAITING but was " + thread.getState());
    }

    private static final class SingleWriterFactory implements BulkWriteClient.BulkStreamWriterFactory {

        private final ControllableWriter writer;
        private boolean createCalled;
        private int createCount;

        private SingleWriterFactory(ControllableWriter writer) {
            this.writer = writer;
        }

        @Override
        public BulkStreamWriter create() {
            createCalled = true;
            createCount++;
            return writer;
        }
    }

    private static final class BlockingFactory implements BulkWriteClient.BulkStreamWriterFactory {

        private final CountDownLatch allowCreate = new CountDownLatch(1);
        private boolean createAttempted;

        @Override
        public BulkStreamWriter create() {
            createAttempted = true;
            try {
                allowCreate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return new ControllableWriter();
        }
    }

    private static final class ControllableWriter implements BulkStreamWriter {

        private final CountDownLatch writeNextStarted = new CountDownLatch(1);
        private final CountDownLatch allowWriteNext = new CountDownLatch(1);
        private final CountDownLatch completedStarted = new CountDownLatch(1);
        private final CountDownLatch allowCompleted = new CountDownLatch(1);
        private RuntimeException writeNextFailure;
        private RuntimeException completedFailure;
        private boolean completedCalled;
        private boolean closed;

        void allowWriteNext() {
            allowWriteNext.countDown();
        }

        void failWriteNext(RuntimeException failure) {
            writeNextFailure = failure;
        }

        void allowCompleted() {
            allowCompleted.countDown();
        }

        void failCompleted(RuntimeException failure) {
            completedFailure = failure;
        }

        @Override
        public Table.TableBufferRoot tableBufferRoot(int maxRows) {
            return new FakeTableBufferRoot();
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            if (writeNextFailure != null) {
                throw writeNextFailure;
            }
            writeNextStarted.countDown();
            try {
                allowWriteNext.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return CompletableFuture.completedFuture(1);
        }

        @Override
        public void completed() {
            if (completedFailure != null) {
                throw completedFailure;
            }
            completedCalled = true;
            closed = true;
            completedStarted.countDown();
            try {
                allowCompleted.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
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
