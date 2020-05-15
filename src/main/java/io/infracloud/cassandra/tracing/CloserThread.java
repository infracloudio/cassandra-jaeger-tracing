package io.infracloud.cassandra.tracing;

import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;

import java.util.LinkedList;
import java.util.List;

/**
 * Since Cassandra does not close traces made by nodes responding to the coordinator,
 * we need to close them manually.
 * <p>
 * This will wait until WAIT_FOR_EVENTS_IN_US microseconds have passed since the
 * last trace() and close the trace manually, with the timestamp of it's last trace.
 */
public class CloserThread extends Thread {

    protected static final long WAIT_FOR_EVENTS_IN_US = 5000000;
    private static final Clock clock = new SystemClock();
    private final List<JaegerTraceState> to_close = new LinkedList<>();
    private boolean started = false;

    public CloserThread() {
        super("TraceCloser");
        setDaemon(true);
    }

    private boolean shouldExpire(JaegerTraceState trace) {
        return clock.currentTimeMicros() - trace.getTimestamp() > (WAIT_FOR_EVENTS_IN_US);
    }

    public void publish(JaegerTraceState trace) {
        synchronized (this) {
            to_close.add(trace);
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

    /**
     * Return whether you managed to close anything
     */
    public boolean process() {
        synchronized (this) {
            for (int i = 0; i < to_close.size(); i++) {
                final JaegerTraceState trace = to_close.get(i);
                // clean up completed traces started as the coordinator
                if (trace.isStopped()) {
                    to_close.remove(i);
                    return true;
                }
                // close the child trace spawned by the coordinator
                if (shouldExpire(trace)) {
                    trace.stop();
                    to_close.remove(i);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void run() {
        while (true) {
            while (process()) {
            }             // while last time something has been closed...
            try {
                sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }
}
