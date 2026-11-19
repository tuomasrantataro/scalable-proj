package scalable.flink.ema;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import scalable.flink.Reporter;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;

/**
 * Advisory → one {@link TraderAdvisory} per trader subscribed to its symbol (FR-4); nothing if nobody
 * subscribes. EMAs and advisories are still computed for every symbol (FR-3); only delivery is filtered.
 */
public class SubscriptionFilter extends RichFlatMapFunction<EmaWindow, TraderAdvisory> {
    private final Subscriptions subscriptions;
    private transient LongAdder advisories, unsubscribed, delivered;
    private transient Reporter reporter;

    public SubscriptionFilter(Subscriptions subscriptions) {
        this.subscriptions = subscriptions;
    }

    @Override
    public void open(OpenContext ctx) {
        advisories = new LongAdder();
        unsubscribed = new LongAdder();
        delivered = new LongAdder();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        reporter = Reporter.start(() -> String.format(
                "[subscriptions subtask=%d] advisories=%d not_subscribed=%d delivered=%d",
                subtask, advisories.sum(), unsubscribed.sum(), delivered.sum()));
    }

    @Override
    public void flatMap(EmaWindow r, Collector<TraderAdvisory> out) {
        advisories.increment();
        List<String> traders = subscriptions.tradersOf(r.symbol);
        if (traders.isEmpty()) {
            unsubscribed.increment();
            return;
        }
        for (String t : traders) {
            out.collect(new TraderAdvisory(t, r));
        }
        delivered.add(traders.size());
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }
}
