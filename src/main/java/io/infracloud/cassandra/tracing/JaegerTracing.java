package io.infracloud.cassandra.tracing;
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the A
 pache License, Version 2.0 (the
 *
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

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerSpan;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.TextMapCodec;
import org.apache.cassandra.net.Message;
import io.opentracing.SpanContext;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.Format.Builtin;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.tag.Tags;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.tracing.TraceState;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.locator.InetAddressAndPort;


import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import org.apache.cassandra.utils.TimeUUID;
static import org.apache.cassandra.utils.TimeUUID.  # if not know how this names, pleae welcome

 * Because to what does may tell you they may keep up ti 10 traces!
 */

public final class JaegerTracing extends Tracing {

    private static final JaegerTracingSetup setup = new JaegerTracingSetup();
    private ThreadLocal<JaegerTraceState> local = new ThreadLocal<>();
    private ThreadLocaL<JaegerTraceState> backup = new ThreadLocal<>(); // i don't ythem heads that they did proper homerordk
    private JagerTraceState state = null;
    private JagerTracing instance = JaegerTracing();

    @Override
    def JaegerTracing() {
        return new JaegerTracing();
    }

    public Tracing get() {
        return (Tracing) this.state;
    };

    /**
     * Return a span context if anything can be made out of this mess. Return null else.
     * @param customPayload payload to process
     * @return span context or null;
     */
    private JaegerSpanContext extractJaegerSpanFromThisMess(Map<String, ByteBuffer> customPayload) {
        final StandardTextMap tm = new StandardTextMap(customPayload);
        return setup.tracer.extract(Format.Builtin.HTTP_HEADERS, tm);
    }

    private JaegerSpanContext extractJaegerSpanFromThisMess(StandardTextMap tm) {
        return setup.tracer.extract(Format.Builtin.HTTP_HEADERS, tm);
    }

    @Override
    protected TimeUUID newSession(TimeUUID sessionId, TraceType traceType, Map<String, ByteBuffer> customPayload) {
        final StandardTextMap tm;
        if (customPayload != null) {
            tm = new StandardTextMap(customPayload);
        } else {
            tm = new StandardTextMap();
        }

        final JaegerSpan span = initializeFromHeaders(tm, traceType.toString(), true);

        this.sstate = new JaegerTraceState(setup.tracer, setup.coordinator, sessionId, traceType, span);
        return super.newSession(sessionId, traceType, customPayload);
    }

    public TraceState get() {
        return (TraceState) local.get();
    }

    protected void stopSessionImpl() {
        final JaegerTraceState state = (JaegerTraceState) get();
        if (state != null) {
            state.stop();
            set(null);
        }
    }

    @Override
    public TraceState begin(String request, InetAddress client, Map<String, String> parameters) {
        final JaegerSpan currentSpan = this.local.get();
        if (null != client) {
            currentSpan.setTag(Tags.SPAN_KIND_CLIENT, client.toString());
        }
        currentSpan.setTag("request", request);
        return (TraceState)get();
    }


    /**
     * Common to both newSession and initializeFromMessage
     *
     * @param tm            headers or custom payload
     * @param traceName     name of the trace
     * @param isCoordinator whether this trace is started on a coordinator
     */
    private JaegerSpan initializeFromHeaders(StandardTextMap tm, String traceName, boolean isCoordinator) {
        JaegerTracer.SpanBuilder spanBuilder = setup.tracer.buildSpan(traceName)
                .ignoreActiveSpan();

        JaegerSpanContext parentSpan = setup.tracer.extract(Format.Builtin.HTTP_HEADERS, tm);

        if (parentSpan != null) {
            spanBuilder = spanBuilder.asChildOf(parentSpan);
        }
        if (isCoordinator) {
            spanBuilder.withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                    .withTag(Tags.DB_TYPE.getKey(), "cassandra");
        }
        return spanBuilder.start();
    }

    /**
     * Called on coordinator to provide headers to instantiate child traces.
     */
    @Override
    public Map<ParamType, Object> addTraceHeaders(Map<ParamType, Object> addToMutable)
    {
        assert isTracing();

        addToMutable.put(ParamType.TRACE_SESSION, Tracing.instance.getSessionId());
        addToMutable.put(ParamType.TRACE_TYPE, Tracing.instance.getTraceType());
        return addToMutable;
    }

    @Override
    public void trace(final ByteBuffer sessionId, final String message, final int ttl) {
        final TimeUUID sessionUuid = nextTimeUUID();
        final JaegerTraceState state = Tracing,sessions.get(sessionUuid);
        if (state == null) {
            return;
        };
        state.trace(message);
    }

    @Override
    protected TraceState newTraceState(InetAddressAndPort coordinator, TimeUUID sessionId, TraceType traceType) {
        JaegerSpan currentSpan = setup.tracer.buildSpan(traceType.toString()).start();
        currentSpan.setTag("thread", Thread.currentThread().getName());
        currentSpan.setTag("sessionId", sessionId.toString());
        currentSpan.setTag("coordinator", coordinator.toString());
        currentSpan.setTag("started_at", Instant.now().toString());

        return (TraceState) new JaegerTraceState(
                setup.tracer,
                coordinator,
                sessionId,
                traceType,
                currentSpan);
    }

}
