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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import java.util.Objects;
import java.util.function.IntSupplier;

final class GreptimeSinkWriterMetrics {

    static final String BUFFER_ROWS = "greptimedb.buffer.rows";
    static final String PENDING_WRITES = "greptimedb.pending.writes";
    static final String FLUSH_TOTAL = "greptimedb.flush.total";
    static final String FLUSH_SUCCESS_TOTAL = "greptimedb.flush.success.total";
    static final String FLUSH_FAILURE_TOTAL = "greptimedb.flush.failure.total";
    static final String FLUSH_ROWS_TOTAL = "greptimedb.flush.rows.total";
    static final String FLUSH_LAST_ROWS = "greptimedb.flush.last.rows";
    static final String FLUSH_LAST_DURATION_MS = "greptimedb.flush.last.duration.ms";
    static final String ASYNC_WRITE_FAILURE_TOTAL = "greptimedb.async.write.failure.total";

    private final IntSupplier bufferedRowsSupplier;
    private final IntSupplier pendingWritesSupplier;
    private final Counter recordsSendCounter;
    private final Counter recordsSendErrorsCounter;
    private final Counter flushTotalCounter;
    private final Counter flushSuccessTotalCounter;
    private final Counter flushFailureTotalCounter;
    private final Counter flushRowsTotalCounter;
    private final Counter asyncWriteFailureTotalCounter;

    private volatile long currentSendTimeMs;
    private volatile long lastFlushRows;
    private volatile long lastFlushDurationMs;

    private GreptimeSinkWriterMetrics(
            SinkWriterMetricGroup metricGroup,
            IntSupplier bufferedRowsSupplier,
            IntSupplier pendingWritesSupplier) {
        this.bufferedRowsSupplier =
                Objects.requireNonNull(bufferedRowsSupplier, "bufferedRowsSupplier must not be null");
        this.pendingWritesSupplier =
                Objects.requireNonNull(pendingWritesSupplier, "pendingWritesSupplier must not be null");

        if (metricGroup == null) {
            this.recordsSendCounter = new SimpleCounter();
            this.recordsSendErrorsCounter = new SimpleCounter();
            this.flushTotalCounter = new SimpleCounter();
            this.flushSuccessTotalCounter = new SimpleCounter();
            this.flushFailureTotalCounter = new SimpleCounter();
            this.flushRowsTotalCounter = new SimpleCounter();
            this.asyncWriteFailureTotalCounter = new SimpleCounter();
            return;
        }

        this.recordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.recordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
        metricGroup.setCurrentSendTimeGauge(() -> currentSendTimeMs);
        metricGroup.gauge(BUFFER_ROWS, () -> (long) this.bufferedRowsSupplier.getAsInt());
        metricGroup.gauge(PENDING_WRITES, () -> (long) this.pendingWritesSupplier.getAsInt());
        metricGroup.gauge(FLUSH_LAST_ROWS, () -> lastFlushRows);
        metricGroup.gauge(FLUSH_LAST_DURATION_MS, () -> lastFlushDurationMs);
        this.flushTotalCounter = metricGroup.counter(FLUSH_TOTAL);
        this.flushSuccessTotalCounter = metricGroup.counter(FLUSH_SUCCESS_TOTAL);
        this.flushFailureTotalCounter = metricGroup.counter(FLUSH_FAILURE_TOTAL);
        this.flushRowsTotalCounter = metricGroup.counter(FLUSH_ROWS_TOTAL);
        this.asyncWriteFailureTotalCounter = metricGroup.counter(ASYNC_WRITE_FAILURE_TOTAL);
    }

    static GreptimeSinkWriterMetrics create(
            SinkWriterMetricGroup metricGroup,
            IntSupplier bufferedRowsSupplier,
            IntSupplier pendingWritesSupplier) {
        return new GreptimeSinkWriterMetrics(metricGroup, bufferedRowsSupplier, pendingWritesSupplier);
    }

    void recordFlushSubmitted(int rows) {
        runMetricUpdate(() -> inc(recordsSendCounter, rows));
    }

    void recordFlushSuccess(int rows, long durationMs) {
        lastFlushRows = rows;
        lastFlushDurationMs = durationMs;
        currentSendTimeMs = durationMs;

        runMetricUpdate(() -> {
            inc(flushTotalCounter);
            inc(flushSuccessTotalCounter);
            inc(flushRowsTotalCounter, rows);
        });
    }

    void recordFlushFailure(int rows, long durationMs) {
        lastFlushRows = rows;
        lastFlushDurationMs = durationMs;
        currentSendTimeMs = durationMs;

        runMetricUpdate(() -> {
            inc(recordsSendErrorsCounter, rows);
            inc(flushTotalCounter);
            inc(flushFailureTotalCounter);
        });
    }

    void recordAsyncWriteFailure() {
        runMetricUpdate(() -> inc(asyncWriteFailureTotalCounter));
    }

    long bufferedRows() {
        return bufferedRowsSupplier.getAsInt();
    }

    long pendingWrites() {
        return pendingWritesSupplier.getAsInt();
    }

    long recordsSend() {
        return recordsSendCounter.getCount();
    }

    long recordsSendErrors() {
        return recordsSendErrorsCounter.getCount();
    }

    long flushTotal() {
        return flushTotalCounter.getCount();
    }

    long flushSuccessTotal() {
        return flushSuccessTotalCounter.getCount();
    }

    long flushFailureTotal() {
        return flushFailureTotalCounter.getCount();
    }

    long flushRowsTotal() {
        return flushRowsTotalCounter.getCount();
    }

    long asyncWriteFailureTotal() {
        return asyncWriteFailureTotalCounter.getCount();
    }

    long lastFlushRows() {
        return lastFlushRows;
    }

    long lastFlushDurationMs() {
        return lastFlushDurationMs;
    }

    private static void inc(Counter counter) {
        counter.inc();
    }

    private static void inc(Counter counter, long amount) {
        counter.inc(amount);
    }

    private static void runMetricUpdate(Runnable update) {
        try {
            update.run();
        } catch (RuntimeException ignored) {
            // Metrics must not change write semantics.
        }
    }
}
