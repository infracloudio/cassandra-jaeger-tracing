package io.infracloud.cassandra.tracing;
import io.jaegertracing.Configuration;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.propagation.TextMapCodec;
import io.opentracing.propagation.Format;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.GlobalOpenTelemetry;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tools.GetVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.config.DatabaseDescriptor;
import io.opentelemetry.api.trace.TracerBuilder;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import io.opentelemetry.sdk.trace.SdkTracerBuilder;
import io.opentelemetry.sdk.trace.SdkTracer;
import io.opentelemetry.opentracingshim.CustomTextMapPropagator;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;

final public class JaegerTracingSetup {
    public static final String DEFAULT_TRACE_KEY = "uber-trace-id";
    private static final String JAEGER_TRACE_KEY_ENV_NAME = "JAEGER_TRACE_KEY";
    private static final String trace_key = (System.getenv(JAEGER_TRACE_KEY_ENV_NAME) == null) ?
            DEFAULT_TRACE_KEY : System.getenv(JAEGER_TRACE_KEY_ENV_NAME);
    public static final Tracer tracer;

    public static final InetAddressAndPort coordinator = FBUtilities.getBroadcastAddressAndPort();

    private static final JaegerSpanExporter() {
        return new JaegerSpanExporter();
    }

    static {
        DatabaseDescriptor.daemonInitialization();

            Resource resource = Resource.getDefault();
                    .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, myName)))
                    .merge(Resource.create(Attributes.of(ResourceAttributes.HOST_NAME, FBUtilities.getJustBroadcastAddress().getHostName())))
                    .merge(Resource.create(Attributes.of(ResourceAttributes.DEPLOYMENT_ENVIRONMENT, DatabaseDescriptor.getClusterName())))
                    .merge(Resource.create(Attributes.of(ResourceAttributes.HOST_IMAGE_VERSION, FBUtilities.getReleaseVersionString)));
        final SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder();
                .addSpanProcessor(BatchSpanProcessor.builder(JaegerGrpcSpanExporter.builder());
        tracer =(Tracer)sdkTracerProvider.build()

    }

}

