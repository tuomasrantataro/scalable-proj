package scalable.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Chained right after the source, so it runs in the same TaskManager as the source reader.
 * Counts items per subtask and per Iggy partition, and stamps which node read each item.
 */
public class NodeCounter extends RichMapFunction<Tick, Tick> {

    private transient LongAdder total;
    private transient Map<Integer, LongAdder> perPartition;
    private transient Counter metric;
    private transient Reporter reporter;
    private transient int subtask;
    private transient String host;

    @Override
    public void open(OpenContext ctx) {
        subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        host = Reporter.hostname();
        total = new LongAdder();
        perPartition = new ConcurrentHashMap<>();
        metric = getRuntimeContext().getMetricGroup().counter("ticksCounted");
        reporter = Reporter.start(() -> String.format("[node-counter subtask=%d host=%s] total=%d perPartition=%s",
                subtask, host, total.sum(), snapshot(perPartition)));
    }

    @Override
    public Tick map(Tick t) {
        total.increment();
        metric.inc();
        perPartition.computeIfAbsent(t.partition, k -> new LongAdder()).increment();
        t.readBySubtask = subtask;
        t.readByHost = host;
        return t;
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }

    static Map<Integer, Long> snapshot(Map<Integer, LongAdder> m) {
        Map<Integer, Long> out = new TreeMap<>();
        m.forEach((k, v) -> out.put(k, v.sum()));
        return out;
    }
}
