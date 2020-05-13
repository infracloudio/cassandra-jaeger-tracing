package io.infracloud.cassandra.tracing;

import io.jaegertracing.internal.clock.Clock;
import io.jaegertracing.internal.clock.SystemClock;

import java.util.LinkedList;
import java.util.List;

import static io.infracloud.cassandra.tracing.JaegerTraceState.WAIT_FOR_EVENTS_IN_MS;

/**
 * Since Cassandra does not close traces made by nodes responding to the coordinator,
 * we need to close them manually.
 *
 * This will wait until WAIT_FOR_EVENTS_IN_MS milliseconds have passed since the
 * last trace() and close the trace manually, with the timestamp of it's last trace.
 */
public class CloserThread extends Thread {
    private final List<JaegerTraceState> to_close = new LinkedList<>();
    private static final Clock clock = new SystemClock();
    private boolean started = false;

    public CloserThread() {
        super("TraceCloser");
        setDaemon(true);
    }

    private boolean shouldExpire(JaegerTraceState trace) {
        return clock.currentTimeMicros() - trace.getTimestamp() > (WAIT_FOR_EVENTS_IN_MS*1000);
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

    public void process() {
        synchronized (this) {
            for (int i = 0; i < to_close.size(); i++) {
                final JaegerTraceState trace = to_close.get(i);
                // clean up traces started by Cassandra
                if (trace.isStopped()) {
                    to_close.remove(i);
                    return; // we return earlier because the meaning of i just changed
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
