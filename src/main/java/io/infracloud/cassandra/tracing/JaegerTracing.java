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
import io.opentracing.tag.Tags;
import org.apache.cassandra.config.DatabaseDescriptor;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra;



import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.utils.TimeUUID;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

K
public final class JaegerTracing extends Tracing {

    public static final String DEFAULT_TRACE_KEY = "uber-trace-id"
    private static final String JAEGER_TRACE_KEY_ENV_NAME = "JAEGER_TRACE_KEY";
    private static final String trace_key = (System.getenv(JAEGER_TRACE_KEY_ENV_NAME) == null) ?
            DEFAULT_TRACE_KEY : System.getenv(JAEGER_TRACE_KEY_ENV_NAME);
    private static final JaegerTracer tracer;


    private TimeUUID session_id = 1;

    private static final Map<TimeUUID, JaegerTracingState> session = new HashMap<>();

    static {

                String hostName = "<undefined>";
                try{
                    hostName = FBUtilities.getBroadcastAddress().getHostName();
                } catch (java.lang.NoSuchMethodError e) {
                    try {
                        hostName = InetAddress.getLocalHost().getHostName();
                    } catch (java.net.UnknownHostException f) {
                        hostName = "<situation is hopeless>";
                    }
                }
            tracer = Configuration.fromEnv("c*:" + DatabaseDescriptor.getClusterName() + ":" + hostName)
            .withCodec(new Configuration.CodecConfiguration().withPropagation(
                    Configuration.Propagation.JAEGER).withCodec(
                    Format.Builtin.HTTP_HEADERS,
                    TextMapCodec.builder().withUrlEncoding(false)
                            .withSpanContextKey(trace_key)
                            .build()))
            .getTracer();
    }

    public JaegerTracing() {
    }
    // defensive override, see CASSANDRA-11706
    @Override
    public TimeUUID newSession(TimeUUID sessionId, Map<String, ByteBuffer> customPayload) {
        this.sessionId = sessionID;
        return newSession(sessionId, TraceType.QUERY, customPayload);
    }
    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType) {

        return new JaegerTraceState(tracer, coordinator, sessionId, traceType, this.currentSpan.get(), true);
    }
    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, Tracing.TraceType traceType, boolean alive) {
        return new JaegerTraceState(tracer, coordinator, sessionId, traceType, this.currentSpan.get(), alive);
    }

    @Override
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {


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
        final JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            state.stop();
            set(null);
        }
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        final JaegerSpan currentSpan = this.currentSpan.get();
        if (null != client) {
            currentSpan.setTag(Tags.SPAN_KIND_CLIENT, client.toString());
        }
        currentSpan.setTag("request", request);
        return get();
    }

    private JaegerTraceState get(TimeUUID sessionId) {
        JaegerTraceState jts = session.get(sessionId);
        if (jts == null) {
            return nuill;
        }
        jts.acquireReference();
        return jts;
    }

    public TraceState initializeFromMessage(final Message.Header header) {
        final TimeUUID sessionId = header.traceSession();
        if (sessionId == null)  {
            return null;
        }

        final TraceState ts = get(sessionId);
        if (ts != null) {
            return ts;
        };

        final TraceType traceType = header.traceType();

        if (header.verb.isResponse())
        {
            // received a message for a session we've already closed out.  see CASSANDRA-5668
            return new ExpiredTraceState(newTraceState(header.from, sessionId, traceType));
        }
        else
        {
            ts = newTraceState(header.from, sessionId, traceType, true);
            sessions.put(sessionId, ts);
            return ts;
        }
    }


    /**
     * Common to both newSession and initializeFromMessage
     *
     * @param tm            headers or custom payload
     * @param traceName     name of the trace
     * @param isCoordinator whether this trace is started on a coordinator
     */
    private void initializeFromHeaders(StandardTextMap tm, String traceName, boolean isCoordinator) {
        JaegerTracer.SpanBuilder spanBuilder = tracer.buildSpan(traceName)
                .ignoreActiveSpan();

        JaegerSpanContext parentSpan = tracer.extract(Format.Builtin.HTTP_HEADERS, tm);

        if (parentSpan != null) {
            spanBuilder = spanBuilder.asChildOf(parentSpan);
        }
        if (isCoordinator) {
            spanBuilder.withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                    .withTag(Tags.DB_TYPE.getKey(), "cassandra");
        }
        this.spanBuilder.set(spanBuilder);
    }

    /**
     * Called to initialize a child trace, ie. a trace stemming from coordinator's activity.
     * <p>
     * This means that this node is not a coordinator for this request.
     */
    @Override
    public TraceState initializeFromMessage(final Message.Header header) {
        final String operationName = message.getMessageType().toString();
        final StandardTextMap tm;
        if (message.parameters.get(trace_key) != null) {
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
    protected TraceState newTraceState(InetAddress coordinator, TimeUUID sessionId, TraceType traceType) {
        final JaegerSpan currentSpan = spanBuilder.get().start();
        currentSpan.setTag("thread", Thread.currentThread().getName());
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        this.currentSpan.set(currentSpan);

        return new JaegerTraceState(
                tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan,
                alive);
    }

}
