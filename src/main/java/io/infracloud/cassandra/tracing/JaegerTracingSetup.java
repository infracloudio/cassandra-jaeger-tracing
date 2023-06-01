package io.infracloud.cassandra.tracing;

import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.BinaryCodec;
import io.jaegertracing.internal.propagation.TextMapCodec;
import io.opentracing.propagation.Format;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

final public class JaegerTracingSetup {
    public static final String DEFAULT_TRACE_KEY = "uber-trace-id";
    public static final JaegerTracer tracer;
    public static final InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();
    private static final String JAEGER_TRACE_KEY_ENV_NAME = "JAEGER_TRACE_KEY";
    private static final String trace_key = (System.getenv(JAEGER_TRACE_KEY_ENV_NAME) == null) ?
            DEFAULT_TRACE_KEY : System.getenv(JAEGER_TRACE_KEY_ENV_NAME);

    static {

        final Configuration.ReporterConfiguration rc = new Configuration.ReporterConfiguration();
        rc.withMaxQueueSize(5);
        rc.withFlushInterval(100);
        tracer = Configuration.fromEnv("c*:" + DatabaseDescriptor.getClusterName() + ":" + FBUtilities.getJustBroadcastAddress().getHostName()).withReporter(rc)
                .withCodec(new Configuration.CodecConfiguration().withBinaryCodec(Format.Builtin.BINARY, BinaryCodec.builder().build()))
                .withCodec(new Configuration.CodecConfiguration().withBinaryCodec(Format.Builtin.BINARY_INJECT, BinaryCodec.builder().build()))
                .withCodec(new Configuration.CodecConfiguration().withBinaryCodec(Format.Builtin.BINARY_EXTRACT, BinaryCodec.builder().build()))
                .withCodec(new Configuration.CodecConfiguration().withPropagation(Configuration.Propagation.JAEGER).withCodec(
                        Format.Builtin.HTTP_HEADERS,
                        TextMapCodec.builder().withUrlEncoding(false)
                                .withSpanContextKey(trace_key)
                                .build()))
                .getTracer();
    }

}

