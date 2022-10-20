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
import io.opentracing.SpanContext;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import java.util.UUID;


final class JaegerTraceState extends TraceState {
    private static final Clock clock = new SystemClock();
    private final JaegerTracer tracer;
    protected final JaegerSpan currentSpan;
    private boolean stopped = false;
    private volatile long timestamp;
    private SpanContext previousTraceContext = null;

    public JaegerTraceState(
            JaegerTracer tracer,
            InetAddressAndPort coordinator,
            UUID sessionId,
            Tracing.TraceType traceType,
            JaegerSpan currentSpan) {
        super(coordinator, sessionId, traceType);
        this.tracer = tracer;
        this.currentSpan = currentSpan;
        timestamp = clock.currentTimeMicros();
    }


    @Override
    protected void traceImpl(String message) {
        // we do it that way because Cassandra calls trace() when an operation completes
        final RegexpSeparator.AnalysisResult analysis = RegexpSeparator.match(message);

        final JaegerTracer.SpanBuilder builder = tracer.buildSpan(analysis.getTraceName())
                .withTag("thread", Thread.currentThread().getName())
                .withStartTimestamp(timestamp)
                .addReference(References.CHILD_OF, currentSpan.context())
                .ignoreActiveSpan();

        if (previousTraceContext != null) {
            builder.addReference(References.FOLLOWS_FROM, previousTraceContext);
        }

        final JaegerSpan span = builder.start();
        analysis.applyTags(span);
        previousTraceContext = span.context();
        span.finish();
        timestamp = clock.currentTimeMicros();
    }

    public boolean isStopped() {
        return stopped;
    }

    @Override
    public void stop() {
        synchronized (this) {
            if (stopped)
                return;
            stopped = true;
        }

        super.stop();
        currentSpan.finish(timestamp);
    }
}
