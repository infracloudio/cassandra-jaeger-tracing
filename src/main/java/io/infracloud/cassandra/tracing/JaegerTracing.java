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

import com.google.common.collect.ImmutableMap;
import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.TextMapCodec;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public final class JaegerTracing extends Tracing {
    // the key mentioned here will be used when sending the call to
    // Cassandra i.e. with customPayload
    public static final String JAEGER_TRACE_KEY = "jaeger-trace";

    private static final Logger logger = LoggerFactory.getLogger(JaegerTracing.class);

    private static final JaegerTracer Tracer = Configuration
            .fromEnv("c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getBroadcastAddress().getHostName())
            .withCodec(new Configuration.CodecConfiguration().withPropagation(
                    Configuration.Propagation.JAEGER).withCodec(
                            Format.Builtin.HTTP_HEADERS,
                            TextMapCodec.builder().withUrlEncoding(true)
                                        .withSpanContextKey(JAEGER_TRACE_KEY)
                                        .build()))
            .getTracer();

    JaegerSpan currentSpan;
    Tracer.SpanBuilder spanBuilder;

    public JaegerTracing() {
    }


    // defensive override, see CASSANDRA-11706
    @Override
    public UUID newSession(UUID sessionId, Map<String, ByteBuffer> customPayload) {
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }


    @Override
    protected UUID newSession(UUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
        ByteBuffer bb = null != customPayload ? customPayload.get(JAEGER_TRACE_KEY) : null;

        if (null != bb) {
            StandardTextMap tm = new StandardTextMap(customPayload);
            JaegerSpanContext parentSpan = Tracer.extract(Format.Builtin.HTTP_HEADERS, tm);

            if (parentSpan == null) {
                logger.error("invalid customPayload in {}", tm.toString());
                spanBuilder = Tracer.buildSpan(traceType.name());
            } else {
                spanBuilder = Tracer.buildSpan(traceType.name()).asChildOf(parentSpan);
            }
        } else {
            spanBuilder = Tracer.buildSpan(traceType.name());
        }

        // the start happens right here
        currentSpan = (JaegerSpan) spanBuilder.start();
        return super.newSession(sessionId, traceType, customPayload);
    }

    @Override
    protected void stopSessionImpl() {
        JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            state.stop();
            currentSpan.finish();
        }
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        if (null != client) {
            currentSpan.setTag("client", client.toString());
        }
        currentSpan.setTag("request", request);
        return get();
    }

    @Override
    public TraceState initializeFromMessage(final MessageIn<?> message) {
        byte[] bytes = message.parameters.get(JAEGER_TRACE_KEY);

        if (null != bytes) {
            StandardTextMap tm = StandardTextMap.from_bytes(message.parameters);

            // TODO; should set these things for a builder and save the builder?
            JaegerSpanContext parentSpan = Tracer.extract(Format.Builtin.HTTP_HEADERS, tm);

            if (parentSpan == null) {
                logger.error("invalid customPayload in {}", tm.toString());
                spanBuilder = Tracer.buildSpan(tm.getOperationName());
            } else {
                spanBuilder = Tracer.buildSpan(message.getMessageType().name()).asChildOf(parentSpan);
            }
        }
        return super.initializeFromMessage(message);
    }

    @Override
    public Map<String, byte[]> getTraceHeaders() {
        assert isTracing();

        return ImmutableMap.<String, byte[]>builder()
                .putAll(super.getTraceHeaders())
                .put(JAEGER_TRACE_KEY, currentSpan.toString().getBytes())
                .build();
    }

    @Override
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        UUID sessionUuid = UUIDGen.getUUID(sessionId);
        TraceState state = Tracing.instance.get(sessionUuid);
        if (state == null) {
            return;
        }
        state.trace(message);
    }

    @Override
    protected TraceState newTraceState(InetAddress coordinator, UUID sessionId, TraceType traceType) {
        // TODO: Span should start here?
        // getServerTracer().setServerReceived();
        currentSpan = (JaegerSpan) spanBuilder.start();
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        return new JaegerTraceState(
                Tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan);
    }

}
