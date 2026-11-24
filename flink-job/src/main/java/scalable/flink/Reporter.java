package scalable.flink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Logs a status line every few seconds, only when it changed. Runs on its own daemon thread. */
public final class Reporter {
    private static final Logger LOG = LoggerFactory.getLogger("scalable.report");
    private static final long INTERVAL_MS = Long.parseLong(TradingJob.envOr("REPORT_INTERVAL_MS", "5000"));

    private final ScheduledExecutorService exec;
    private final Supplier<String> status;
    private String last = "";

    private Reporter(Supplier<String> status) {
        this.status = status;
        this.exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "reporter");
            t.setDaemon(true);
            return t;
        });
        exec.scheduleAtFixedRate(this::report, INTERVAL_MS, INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public static Reporter start(Supplier<String> status) {
        return new Reporter(status);
    }

    private synchronized void report() {
        String s = status.get();
        if (!s.equals(last)) {
            LOG.info(s);
            last = s;
        }
    }

    public void stop() {
        exec.shutdownNow();
        report();
    }
}
