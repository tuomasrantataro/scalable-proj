package scalable.flink.ema;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scalable.flink.Reporter;

import java.util.concurrent.atomic.LongAdder;

/**
 * Passes usable price events on and reports suspicious ones (side output {@link #ISSUES}).
 *
 * <ul>
 *   <li>Dropped: zero prices (counted only), malformed rows, negative prices, implausible event dates.</li>
 *   <li>Kept and reported: `Trading date` ≠ `Date`, and `Trading time` more than {@code skewMs} away from the
 *       system time of the row.</li>
 * </ul>
 */
public class QualityGate extends ProcessFunction<MarketEvent, MarketEvent> {
    public static final OutputTag<DataIssue> ISSUES = new OutputTag<>("data-issues", TypeInformation.of(DataIssue.class));

    private final long skewMs;
    private transient IssueCounts counts;
    private transient LongAdder usable;
    private transient Reporter reporter;

    public QualityGate(long skewMs) {
        this.skewMs = skewMs;
    }

    @Override
    public void open(OpenContext ctx) {
        counts = new IssueCounts();
        usable = new LongAdder();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        reporter = Reporter.start(() -> String.format("[quality subtask=%d] usable=%d issues:%s",
                subtask, usable.sum(), counts.format()));
    }

    @Override
    public void processElement(MarketEvent e, Context ctx, Collector<MarketEvent> out) {
        if (!e.usable()) {
            IssueType type = IssueType.valueOf(e.reject);
            counts.add(type);
            if (type != IssueType.ZERO_PRICE) {
                ctx.output(ISSUES, DataIssue.of(type, e, e.rejectDetail));
            }
            return;
        }
        if (e.tradingDateDiffers) {
            counts.add(IssueType.TRADING_DATE_MISMATCH);
            ctx.output(ISSUES, DataIssue.of(IssueType.TRADING_DATE_MISMATCH, e, "Trading date differs from Date"));
        }
        if (e.systemTime != Long.MIN_VALUE && Math.abs(e.eventTime - e.systemTime) > skewMs) {
            counts.add(IssueType.TIME_SKEW);
            long s = (e.eventTime - e.systemTime) / 1000;
            ctx.output(ISSUES, DataIssue.of(IssueType.TIME_SKEW, e,
                    String.format("Trading time is %d s %s the system time", Math.abs(s), s > 0 ? "after" : "before")));
        }
        usable.increment();
        out.collect(e);
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }
}
