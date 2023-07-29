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
import io.opentracing.SpanContext;
import io.opentracing.propagation.Binary;
import io.opentracing.propagation.BinaryInject;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.TimeUUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * How does it work?
 *
 * There are a number of traces that Cassandra can emit:
 *
 * 1. TraceType.REPAIR
 *      a. Starts with a call to Tracing.newSession(TraceType.REPAIR)
 *      b.
 */
public final class JaegerTracing extends Tracing {

    private static final String JAEGER_HEADER = "jaeger-header";
    private static final JaegerTracingSetup setup = new JaegerTracingSetup();

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);


    /* a pusty descriptor is necessary for Cassandra to initialize this class **/
    public JaegerTracing() {}

    /**
     * Stop the session processed by the current thread
     */
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
        this.logger.trace("begin({}, {}, {})", request, client, parameters.toString());
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
        this.logger.trace("newSession({}, {})", sessionId, traceType.toString());
        JaegerSpanContext parentSpan = JaegerTracingSetup.tracer.extract(Format.Builtin.HTTP_HEADERS, map);
        // no need to trace if the parent is not sampled as well, aight?
        if (!parentSpan.isSampled()) {
            parentSpan = null;
        }

        // this is valid even when parentSpan is null
        final TraceState ts = newTraceState(JaegerTracingSetup.coordinator, sessionId, traceType, parentSpan);
        set(ts);
        sessions.put(sessionId, ts);
        return sessionId;
    }

    @Override
    /**
     * Called for non-local traces (traces that are not initiated by local node == coordinator).
     */
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        this.logger.trace("trace({}, {})", sessionId, message, ttl);
        final TimeUUID sessionUuid = TimeUUID.deserialize(sessionId);
        final TraceState state = sessions.get(sessionUuid);
        if (state == null) {
            return;
        }
        state.trace(message);
    }

    @Override
    public TraceState initializeFromMessage(Message.Header header) {
        if (header.customParams() != null) {
            final byte[] bytes = header.customParams().get(JAEGER_HEADER);

            if (bytes != null) {
                logger.trace("Seen {} long header", bytes.length);
                // I did not write this using tracer's .extract and .inject() because I'm a Java noob - @piotrmaslanka
                final BinaryString bt = new JaegerTracing.BinaryString(bytes);
                final BinaryCodec bc = new BinaryCodec();
                final JaegerSpanContext context = bc.extract(bt);
                final JaegerTracer.SpanBuilder builder = JaegerTracingSetup.tracer.buildSpan(header.verb.toString()).asChildOf(context);
                final JaegerSpan span = builder.start();
                return new JaegerTraceState(JaegerTracingSetup.tracer, header.from, header.traceSession(), header.traceType(),
                        span);
            }
        }
        return super.initializeFromMessage(header);
    }

    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable) {
        final Span span = JaegerTracingSetup.tracer.activeSpan();
        if (span != null) {
            final BinaryString bin_str = new BinaryString();
            final BinaryCodec bc = new BinaryCodec();
            bc.inject(((JaegerSpanContext)span.context()), bin_str);

            addToMutable.put(
                    ParamType.CUSTOM_MAP,
                    new LinkedHashMap<String, byte[]>() {{
                        put(JAEGER_HEADER, bin_str.bytes);
                    }});
        }
        return super.addTraceHeaders(addToMutable);
    }

    private TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType,
                                     JaegerSpanContext span) {
        Span my_span = JaegerTracingSetup.tracer.activeSpan();
        JaegerTracer.SpanBuilder sb = JaegerTracingSetup.tracer.buildSpan(traceType.toString());
        if (my_span != null) {
            sb = sb.addReference(References.FOLLOWS_FROM, my_span.context());
        }
        if (span != null) {
            sb = sb.addReference(References.CHILD_OF, span);
        }
        JaegerSpan currentSpan = sb.start();
        currentSpan.setTag("thread", Thread.currentThread().getName());
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        final TraceState ts = new JaegerTraceState(
                JaegerTracingSetup.tracer,
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

    public static class BinaryString extends BinaryCodec implements Binary {
        public byte[] bytes;

        public BinaryString() {
            this.bytes = new byte[64];
        }

        public BinaryString(byte[] arg) {
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

}
