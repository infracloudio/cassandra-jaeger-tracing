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

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.TextMapCodec;
import io.opentracing.SpanContext;
import io.opentracing.propagation.Format;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A single instance of this is created for entire Cassandra, so we need to use thread-local
 * storage.
 *
 * Now, there are two possibilities. Either we are the coordinator, in which case Cassandra will call:
 * 1. newSession(...)
 * 2.   which is called by (1). newTraceState(...)
 * 3. begin(...)
 * 4. trace(...)
 * 5. stopSessionImpl(...)
 *
 * or we are a replica, responding to a coordinator, in which case it will look more like:
 *
 * 1. initializeFromMessage(...) by MessagingService
 * 1.    which is called by (1) newTraceState(...)
 * 2. trace(...)
 * 3. ...
 * 4. Nothing. Dead silence. Cassandra doesn't tell us when such a session has finished!
 *
 * So we'll spawn a thread (CloserThread) to close them for us automatically.
 *
 * So in general, in newSession/initializeFromMessage we prepare the builder, then in
 * newTrace start the span, and hope for the best.
 */
public final class JaegerTracing extends Tracing {
    /**
     * The key mentioned here will be used when sending the call to Cassandra with customPayload.
     * Encode it using HTTP codec with url_encode=true
     */
    public static final String JAEGER_TRACE_KEY = "jaeger-trace";

    private static final JaegerTracer tracer = Configuration
            .fromEnv("c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getBroadcastAddress().getHostName())
            .withCodec(new Configuration.CodecConfiguration().withPropagation(
                    Configuration.Propagation.JAEGER).withCodec(
                            Format.Builtin.HTTP_HEADERS,
                            TextMapCodec.builder().withUrlEncoding(false)
                                        .withSpanContextKey(JAEGER_TRACE_KEY)
                                        .build()))
            .getTracer();

    // Since Cassandra spawns a single JaegerTracing instance, we need to make use
    // of thread locals so as not to get confused.
    final ThreadLocal<JaegerSpan> currentSpan = new ThreadLocal<>();
    final ThreadLocal<JaegerTracer.SpanBuilder> spanBuilder = new ThreadLocal<>();

    public JaegerTracing() {
    }


    // defensive override, see CASSANDRA-11706
    @Override
    public UUID newSession(UUID sessionId, Map<String, ByteBuffer> customPayload) {
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }

    @Override
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
        final StandardTextMap tm;
        if (customPayload != null) {
            tm = new StandardTextMap(customPayload);
        } else {
            tm = new StandardTextMap();
        }
        initializeFromHeaders(tm, traceType.name(), true);
        return super.newSession(sessionId, traceType, customPayload);
    }

    @Override
    protected void stopSessionImpl() {
        JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            state.stop();
            set(null);
        }
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        final JaegerSpan currentSpan = this.currentSpan.get();
        if (null != client) {
            currentSpan.setTag("client", client.toString());
        }
        currentSpan.setTag("request", request);
        return get();
    }

    /**
     * Common to both newSession and initializeFromMessage
     * @param tm headers or custom payload
     * @param traceName name of the trace
     */
    private void initializeFromHeaders(StandardTextMap tm, String traceName, boolean isCoordinator) {
        JaegerTracer.SpanBuilder spanBuilder = tracer.buildSpan(traceName)
                                                     .ignoreActiveSpan();

        JaegerSpanContext parentSpan = tracer.extract(Format.Builtin.HTTP_HEADERS, tm);

        if (parentSpan != null) {
            spanBuilder = spanBuilder.asChildOf(parentSpan);
        }
        if (isCoordinator) {
            spanBuilder.withTag("span.kind", "server").withTag("db.type", "cassandra");
        }
        this.spanBuilder.set(spanBuilder);
    }

    /**
     * Called to initialize a child trace, ie. a trace stemming from coordinator's activity.
     *
     * This means that this node is not a coordinator for this request.
     */
    @Override
    public TraceState initializeFromMessage(final MessageIn<?> message) {
        final String operationName = message.getMessageType().toString()+" [" + Thread.currentThread().getName() + "]";
        final StandardTextMap tm;
        if (message.parameters.get(JAEGER_TRACE_KEY) != null) {
            tm = StandardTextMap.from_bytes(message.parameters);
        } else {
            tm = StandardTextMap.EMPTY_MAP;
        }
        initializeFromHeaders(tm, operationName, false);
        return super.initializeFromMessage(message);
    }

    /**
     * Called on coordinator to provide headers to instantiate child traces.
     */
    @Override
    public Map<String, byte[]> getTraceHeaders() {
        if (!(isTracing() && currentSpan.get() != null)) {
            return super.getTraceHeaders();
        }

        final Map<String, byte[]> map = new HashMap<>();
        final Map<String, byte[]> headers = super.getTraceHeaders();
        for (Map.Entry<String, byte[]> entry : headers.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        final StandardTextMap stm = new StandardTextMap();
        final SpanContext context = currentSpan.get().context();
        tracer.inject(context, Format.Builtin.HTTP_HEADERS, stm);
        stm.injectToByteMap(map);
        return map;
    }

    @Override
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        final UUID sessionUuid = UUIDGen.getUUID(sessionId);
        final TraceState state = Tracing.instance.get(sessionUuid);
        if (state == null) {
            return;
        }
        state.trace(message);
    }

    @Override
    protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType) {
        final JaegerSpan currentSpan = spanBuilder.get().start();
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        this.currentSpan.set(currentSpan);

        return new JaegerTraceState(
                tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan);
    }

}
