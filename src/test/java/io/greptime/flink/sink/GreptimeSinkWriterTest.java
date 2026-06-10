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
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GreptimeSinkWriterTest {

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

    private static GreptimeSinkWriter<String> newSinkWriter(FakeBulkStreamWriter bulkWriter, int batchSize) {
        return new GreptimeSinkWriter<>(
                null,
                bulkWriter,
                value -> new Object[] { value },
                batchSize);
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

    private static final class FakeBulkStreamWriter implements BulkStreamWriter {

        private final Queue<CompletableFuture<Integer>> writes = new ArrayDeque<>();

        void enqueueWrite(CompletableFuture<Integer> future) {
            writes.add(future);
        }

        @Override
        public Table.TableBufferRoot tableBufferRoot(int maxRows) {
            return new FakeTableBufferRoot();
        }

        @Override
        public CompletableFuture<Integer> writeNext() {
            CompletableFuture<Integer> future = writes.poll();
            if (future == null) {
                return CompletableFuture.completedFuture(1);
            }
            return future;
        }

        @Override
        public void completed() {
        }

        @Override
        public void close() {
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
