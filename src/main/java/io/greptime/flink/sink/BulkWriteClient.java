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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wraps {@link BulkStreamWriter} SDK calls (stream creation, {@code writeNext()},
 * {@code completed()}) inside timeout-protected boundaries so that a stuck gRPC call
 * cannot permanently block the TaskManager write thread, checkpoint, or close.
 *
 * <p>Every blocking SDK call is executed on a dedicated single-thread daemon executor.
 * If the call does not return within the configured timeout, the corresponding
 * {@link Future} is cancelled and the underlying {@link BulkStreamWriter} is forcibly
 * closed to release the underlying gRPC resources.
 */
final class BulkWriteClient {

    private static final AtomicLong THREAD_ID = new AtomicLong();

    private final BulkStreamWriterFactory writerFactory;
    private final ExecutorService executor;
    private final Runnable shutdownClient;
    private BulkStreamWriter writer;
    private boolean streamClosed = true;

    BulkWriteClient(BulkStreamWriterFactory writerFactory, Runnable shutdownClient) {
        this.writerFactory = Objects.requireNonNull(writerFactory, "writerFactory");
        this.shutdownClient = Objects.requireNonNull(shutdownClient, "shutdownClient");
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "greptimedb-bulk-write-" + THREAD_ID.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    Table.TableBufferRoot newTableBuffer(int columnBufferSize) {
        return currentWriter().tableBufferRoot(columnBufferSize);
    }

    void ensureStreamOpen(long timeoutMs) throws Exception {
        if (hasOpenStream()) {
            return;
        }

        AtomicBoolean cancelled = new AtomicBoolean();
        Callable<Void> openTask = () -> {
            BulkStreamWriter openedWriter = writerFactory.create();
            boolean shouldClose;
            synchronized (this) {
                if (cancelled.get()) {
                    shouldClose = true;
                } else {
                    setWriter(openedWriter);
                    shouldClose = false;
                }
            }
            if (shouldClose) {
                closeWriterQuietly(openedWriter);
            }
            return null;
        };
        Future<Void> opening = executor.submit(openTask);
        try {
            opening.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cancelled.set(true);
            opening.cancel(true);
            throw newStreamOpenTimeoutException(timeoutMs);
        } catch (InterruptedException e) {
            cancelled.set(true);
            opening.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    CompletableFuture<Integer> writeNext(long timeoutMs) throws Exception {
        BulkStreamWriter w = currentWriter();
        Future<CompletableFuture<Integer>> invocation = executor.submit(w::writeNext);
        try {
            return invocation.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            invocation.cancel(true);
            TimeoutException te = newWriteNextTimeoutException(timeoutMs);
            closeStreamSuppressing(te);
            throw te;
        } catch (InterruptedException e) {
            invocation.cancel(true);
            closeStreamSuppressing(e);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    void completed(long timeoutMs) throws Exception {
        BulkStreamWriter w = currentWriter();
        Future<Void> completion = executor.submit(() -> {
            w.completed();
            return null;
        });
        try {
            completion.get(timeoutMs, TimeUnit.MILLISECONDS);
            markStreamClosed(w);
        } catch (TimeoutException e) {
            completion.cancel(true);
            TimeoutException te = newCompletedTimeoutException(timeoutMs);
            closeStreamSuppressing(te);
            throw te;
        } catch (InterruptedException e) {
            completion.cancel(true);
            closeStreamSuppressing(e);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            throw unwrapExecutionException(e);
        }
    }

    synchronized void closeStream() throws Exception {
        if (writer == null || streamClosed) {
            return;
        }
        try {
            writer.close();
        } finally {
            writer = null;
            streamClosed = true;
        }
    }

    void shutdown() {
        try {
            closeStream();
        } catch (Exception ignored) {
            // The stream may already be broken (timed-out, cancelled, or already
            // closed). Whether closeStream succeeds or fails, the executor and
            // the underlying client must still be shut down.
        } finally {
            executor.shutdownNow();
            shutdownClient.run();
        }
    }

    private synchronized BulkStreamWriter currentWriter() {
        if (writer == null || streamClosed) {
            throw new IllegalStateException("GreptimeDB bulk write stream is not open");
        }
        return writer;
    }

    private synchronized boolean hasOpenStream() {
        return writer != null && !streamClosed;
    }

    private synchronized void setWriter(BulkStreamWriter w) {
        writer = Objects.requireNonNull(w, "writer");
        streamClosed = false;
    }

    private synchronized void markStreamClosed(BulkStreamWriter w) {
        if (writer == w) {
            writer = null;
            streamClosed = true;
        }
    }

    private void closeStreamSuppressing(Throwable cause) {
        try {
            closeStream();
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    private static void closeWriterQuietly(BulkStreamWriter writer) {
        try {
            writer.close();
        } catch (Exception ignored) {
            // The writer is being discarded because the stream-open operation
            // already timed out. Its close may fail due to the same underlying
            // issue (unreachable server, broken gRPC channel), and there is no
            // caller to propagate the exception to. Swallow it so the executor
            // task can exit cleanly.
        }
    }

    private static TimeoutException newStreamOpenTimeoutException(long timeoutMs) {
        return new TimeoutException(
                "Timed out after " + timeoutMs + " ms while opening GreptimeDB bulk stream");
    }

    private static TimeoutException newWriteNextTimeoutException(long timeoutMs) {
        return new TimeoutException(
                "Timed out after " + timeoutMs + " ms while invoking GreptimeDB bulk writeNext()");
    }

    private static TimeoutException newCompletedTimeoutException(long timeoutMs) {
        return new TimeoutException(
                "Timed out after " + timeoutMs + " ms while invoking GreptimeDB bulk completed()");
    }

    private static Exception unwrapExecutionException(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        return e;
    }

    @FunctionalInterface
    interface BulkStreamWriterFactory {
        BulkStreamWriter create();
    }
}
