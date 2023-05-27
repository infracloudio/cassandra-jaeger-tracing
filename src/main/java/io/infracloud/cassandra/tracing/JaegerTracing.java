package io.infracloud.cassandra.tracing;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
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

import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.opentracing.References;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;


public final class JaegerTracing extends Tracing {

    private static final JaegerTracingSetup setup = new JaegerTracingSetup();

    public JaegerTracing() {
    }

    @Override
    protected void stopSessionImpl() {
        final JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            state.stop();
            set(null);
        }
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        JaegerTraceState state = (JaegerTraceState) get();
        if (state == null) {
            final StandardTextMap tm = StandardTextMap.copyFrom(parameters);
            final TimeUUID traceStateUUID = newSession(TimeUUID.Generator.nextTimeUUID(), TraceType.REPAIR, tm.toByteBuffer());
            state = (JaegerTraceState) sessions.get(traceStateUUID);
            state.currentSpan.setTag("request", request);
        }
        if (client != null) {
            state.currentSpan.setTag(Tags.SPAN_KIND_CLIENT, client.toString());
        }
        set(state);
        return (TraceState) state;
    }

    @Override
    /**
     * This is meant to be used by implementations that need access to the message payload to begin their tracing.
     *
     * Since our tracing headers are to be found within the customPayload, this use case is warranted.
     *
     * @param customPayload note that this might be null
     */
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
        final StandardTextMap map = new StandardTextMap(customPayload);
        JaegerSpanContext parentSpan = setup.tracer.extract(Format.Builtin.HTTP_HEADERS, map);
        // no need to trace if the parent is not sampled as well, aight?
        if (!parentSpan.isSampled()) {
            parentSpan = null;
        }

        final TraceState ts;
        if (parentSpan != null) {
            ts = newTraceState(this.setup.coordinator, sessionId, traceType, parentSpan);
        } else {
            ts = newTraceState(this.setup.coordinator, sessionId, traceType);
        }
        set(ts);
        sessions.put(sessionId, ts);
        return sessionId;
    }

    @Override
    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        final TimeUUID sessionUuid = TimeUUID.deserialize(sessionId);
        final TraceState state = sessions.get(sessionUuid);
        if (state == null) {
            return;
        }
        ;
        state.trace(message);
    }

    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType,
                                       JaegerSpanContext span) {
        JaegerTracer.SpanBuilder sb = setup.tracer.buildSpan(traceType.toString());
        if (span != null) {
            sb = sb.addReference(References.CHILD_OF, span);
        }
        JaegerSpan currentSpan = sb.start();
        currentSpan.setTag("thread", Thread.currentThread().getName());
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        final TraceState ts = new JaegerTraceState(
                setup.tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan);
        return ts;
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType) {
        return newTraceState(coordinator, sessionId, traceType, null);
    }

}
