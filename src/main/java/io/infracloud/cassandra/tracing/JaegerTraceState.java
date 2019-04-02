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
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;

import java.net.InetAddress;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
// import java.util.concurrent.TimeUnit;

final class JaegerTraceState extends TraceState
{
    private final JaegerTracer Tracer;
    private final JaegerSpan currentSpan;

    // re-using property from TraceStateImpl.java
    private static final int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
      Integer.valueOf(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "1"));

    final Deque<JaegerSpan> openSpans = new ConcurrentLinkedDeque();
    private final ThreadLocal<JaegerSpan> localSpan = new ThreadLocal<>();

    public JaegerTraceState(
			    JaegerTracer Tracer,
			    InetAddress coordinator,
			    UUID sessionId,
			    Tracing.TraceType traceType,
			    JaegerSpan currentSpan)
    {
	super(coordinator, sessionId, traceType);
	assert null != currentSpan;
	this.Tracer = Tracer;
	this.currentSpan = currentSpan;
    }

    @Override
    protected void traceImpl(String message)
    {
        traceImplWithClientSpans(message);
    }

    void close()
    {
        // brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);
        closeClientSpans();
    }

    private void traceImplWithClientSpans(String message)
    {
        // brave.serverSpanThreadBinder().setCurrentSpan(serverSpan);
        if (null != localSpan.get())
        {
            // brave.clientTracer().setClientReceived();
	    localSpan.get().finish();
            openSpans.remove(localSpan.get());
            localSpan.remove();
        }
	JaegerSpan span = Tracer.buildSpan(message + " [" + Thread.currentThread().getName() + "]").start();
	// brave.clientTracer().startNewSpan(message + " [" + Thread.currentThread().getName() + "]");
        // brave.clientTracer().setClientSent();
        // Span prev = brave.clientSpanThreadBinder().getCurrentClientSpan();
        // currentSpan.set(prev);
        // openSpans.addLast(prev);
	localSpan.set(span);
	openSpans.addLast(span);
    }

    private void closeClientSpans()
    {
        for (JaegerSpan span : openSpans)
        {
	    span.finish();
        }
        openSpans.clear();
        // currentSpan.remove();
	localSpan.remove();
    }

    @Override
    protected void waitForPendingEvents() {
        int sleepTime = 100;
        int maxAttempts = WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS / sleepTime;
        for (int i = 0; 0 < openSpans.size() && i < maxAttempts ; ++i)
        {
            try
            {
                Thread.sleep(sleepTime);
            }
            catch (InterruptedException ex)
            {
            }
        }
    }
}
