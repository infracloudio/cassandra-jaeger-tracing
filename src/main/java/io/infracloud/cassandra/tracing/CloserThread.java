package io.infracloud.cassandra.tracing;

import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;

import java.util.LinkedList;
import java.util.List;

/**
 * Since Cassandra does not close traces made by nodes responding to the coordinator,
 * we need to close them manually.
 */
public class CloserThread extends Thread {
    private final List<JaegerTraceState> to_close = new LinkedList<>();
    private static final Clock clock = new SystemClock();
    private boolean started = false;
    // close traces after 3 seconds of inactivity
    private static long EXPIRATION_TIME_IN_US = 3000000;

    public CloserThread() {
        super();
        setName("TraceCloser");
        setDaemon(true);
    }

    private boolean shouldExpire(JaegerTraceState trace) {
        return clock.currentTimeMicros() - trace.getTimestamp() > EXPIRATION_TIME_IN_US;
    }

    public void publish(JaegerTraceState trace) {
        synchronized (this) {
            to_close.add(trace);
        }
    }

    public void process() {
        synchronized (this) {
            for (int i = 0; i < to_close.size(); i++) {
                final JaegerTraceState trace = to_close.get(i);
                // clean up traces started by Cassandra
                if (trace.isStopped()) {
                    to_close.remove(i);
                    return;
                }
                // conditionally close trace spawned by the coordinator
                if (shouldExpire(trace)) {
                    trace.dontWaitUponClose();
                    trace.stop();
                    to_close.remove(i);
                    return;
                }
            }
        }
    }

    @Override
    public void start() {
        synchronized (this) {
            if (started)
                return;
            started = true;
        }
        super.start();
    }

    public void run() {
        while (true) {
            process();
            if (to_close.size() == 0) {
                try {
                    sleep(1000);
                } catch (InterruptedException e) {
                }
            }
        }
    }

}
