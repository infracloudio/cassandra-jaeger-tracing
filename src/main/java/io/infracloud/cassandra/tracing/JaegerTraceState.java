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
package io.infracloud.cassandra.tracing;

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;
import io.opentracing.References;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;


final class JaegerTraceState extends TraceState {
    private static final Clock clock = new SystemClock();
    private static final int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS = 60;
    private final ThreadLocal<JaegerSpan> currentSpan = new ThreadLocal<>();
    final Deque<JaegerSpan> openSpans = new ConcurrentLinkedDeque();
    private final JaegerTracer tracer;

    private static final Logger logger = LoggerFactory.getLogger(JaegerTraceState.class);

    JaegerSpan span;
    private volatile long timestamp;
    private boolean stopped = false;

    public JaegerTraceState(
            JaegerTracer tracer,
            InetAddressAndPort coordinator,
            TimeUUID sessionId,
            Tracing.TraceType traceType,
            JaegerSpan currentSpan) {
        super(coordinator, sessionId, traceType);
        this.tracer = tracer;
        if (currentSpan != null) {
            this.openSpans.add(currentSpan);
            this.span = currentSpan;
        }
        timestamp = clock.currentTimeMicros();
    }

    @Override
    protected void traceImpl(String message) {
        // we do it that way because Cassandra calls trace() when an operation completes, not when it starts
        // as is expected by Jaeger
        final JaegerSpan child_of = this.currentSpan.get();
        if (this.span != null) {
            this.tracer.activateSpan(this.span);
        }
        final RegexpSeparator.AnalysisResult analysis = RegexpSeparator.match(message);

        JaegerTracer.SpanBuilder builder = tracer.buildSpan(analysis.getTraceName())
                .withTag("thread", Thread.currentThread().getName())
                .withStartTimestamp(timestamp)
                .ignoreActiveSpan();
        if (child_of != null) {
            builder = builder.addReference(References.CHILD_OF, child_of.context());
        }

        if (this.span != null) {
            builder.addReference(References.FOLLOWS_FROM, this.span.context());
        }

        final JaegerSpan span = builder.start();
        analysis.applyTags(span);
        if (child_of != null) {
            child_of.finish();
            this.openSpans.remove(child_of);
            this.currentSpan.remove();
        }
        this.currentSpan.set(span);
        this.span = span;
        timestamp = clock.currentTimeMicros();
    }

    private void closeClientSpans() {
        for (final JaegerSpan span : this.openSpans) {
            this.tracer.activateSpan(span);
            span.finish();
        }
        this.openSpans.clear();
        this.currentSpan.remove();
        this.span = null;
    }

    @Override
    protected void waitForPendingEvents() {
        int sleepTime = 100;
        int maxAttempts = WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS / sleepTime;
        for (int i = 0; 0 < openSpans.size() && i < maxAttempts; ++i) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ex) {
            }
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            if (stopped)
                return;
            stopped = true;
        }

        closeClientSpans();
        super.stop();
    }
}
