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

import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;
import io.opentracing.References;
import io.opentracing.SpanContext;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.UUID;

final class JaegerTraceState extends TraceState
{

    private final JaegerTracer tracer;
    private final JaegerSpan currentSpan;
    private boolean stopped = false;
    private volatile long timestamp;
    private static final Clock clock = new SystemClock();
    private volatile JaegerSpan currentTrace;

    public JaegerTraceState(
			    JaegerTracer Tracer,
			    InetAddress coordinator,
			    UUID sessionId,
			    Tracing.TraceType traceType,
			    JaegerSpan currentSpan)
    {
        super(coordinator, sessionId, traceType);
        assert null != currentSpan;
        tracer = Tracer;
        this.currentSpan = currentSpan;
    }

    @Override
    protected void traceImpl(String message)
    {
        traceImplWithClientSpans(message);
    }

    private void traceImplWithClientSpans(String message)
    {
        JaegerTracer.SpanBuilder builder = tracer.buildSpan(message + " [" + Thread.currentThread().getName() + "]")
                                                 .addReference(References.CHILD_OF, (SpanContext) currentSpan.context())
                                                 .ignoreActiveSpan();
        if (currentTrace != null) {
            currentTrace.finish();
            builder = builder.addReference(References.FOLLOWS_FROM, (SpanContext)currentTrace.context());
        }

        currentTrace = builder.start();
        timestamp = clock.currentTimeMicros();
    }

    @Override
    public void stop() {
        if (stopped)
            return;
        timestamp = clock.currentTimeMicros();

        // close all of the spans that we had to close
        if (currentTrace != null) {
            currentTrace.finish();
            currentTrace = null;
        }
        stopped = true;
        super.stop();
        currentSpan.finish(timestamp);
    }

    @Override
    public void waitForPendingEvents() {
        try {
            Thread.currentThread().sleep(2000);
        } catch (InterruptedException e) {
        }
    }
}
