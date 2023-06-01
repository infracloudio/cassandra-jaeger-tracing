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
import io.jaegertracing.internal.propagation.BinaryCodec;
import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.*;
import io.opentracing.tag.Tags;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;


public final class JaegerTracing extends Tracing {

    private static final String JAEGER_HEADER = "jaeger-header";
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
            state.span.setTag("request", request);
        }
        if (client != null) {
            state.span.setTag(Tags.SPAN_KIND_CLIENT, client.toString());
        }
        set(state);
        return state;
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
        // this is valid even when parentSpan is None
        ts = newTraceState(this.setup.coordinator, sessionId, traceType, parentSpan);
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

    public static class BinaryString extends BinaryCodec implements BinaryInject, Binary {
        public byte[] bytes;

        BinaryString() {
            this.bytes = new byte[64];
        }

        BinaryString(byte[] arg) {
            this.bytes = arg;
        }


        @Override
        public ByteBuffer injectionBuffer(int i) {
            return ByteBuffer.wrap(this.bytes);
        }

        @Override
        public ByteBuffer extractionBuffer() {
            return ByteBuffer.wrap(this.bytes);
        }
    }

    @Override
    public TraceState initializeFromMessage(Message.Header header) {
        byte[] bytes = null;
        if (header.customParams() != null) {
            bytes = header.customParams().get(JAEGER_HEADER);

            if (bytes != null) {
                // I did not write this using tracer's .extract and .inject() because I'm a java noob - @piotrmaslanka
                final BinaryString bt = new JaegerTracing.BinaryString(bytes);
                final BinaryCodec bc = new BinaryCodec();
                final JaegerSpanContext ctxt = bc.extract(bt);
                final JaegerTracer.SpanBuilder builder = this.setup.tracer.buildSpan(header.verb.toString()).asChildOf(ctxt);
                final JaegerSpan span = builder.start();
                return new JaegerTraceState(this.setup.tracer, header.from, header.traceSession(), header.traceType(),
                        span);
            }
        }
        return super.initializeFromMessage(header);
    }

    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        final Span span = setup.tracer.activeSpan();
        if (span != null) {
            final BinaryCodec bt = new BinaryCodec();
            final BinaryString bs = new BinaryString();
            bt.inject(((JaegerSpanContext)span.context()), bs);

            addToMutable.put(
                    ParamType.CUSTOM_MAP,
                    new HashMap<String, byte[]>() {{
                        put(JAEGER_HEADER, bs.bytes);
                    }});
        }
        return super.addTraceHeaders(addToMutable);
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
