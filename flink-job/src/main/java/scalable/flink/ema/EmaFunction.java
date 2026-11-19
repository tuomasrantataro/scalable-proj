package scalable.flink.ema;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scalable.flink.Reporter;

import java.util.concurrent.atomic.LongAdder;

/**
 * Keyed by symbol: applies each closed window to the symbol's {@link EmaState} (EMA38, EMA100, crossovers)
 * and emits an {@link EmaWindow}. Window results of one key arrive in window order, because event-time
 * windows fire as the watermark passes their end. Reports price jumps between consecutive closes.
 */
public class EmaFunction extends KeyedProcessFunction<String, PriceWindow, EmaWindow> {
    public static final OutputTag<DataIssue> ISSUES = new OutputTag<>("ema-issues", TypeInformation.of(DataIssue.class));

    private final double jumpRatio;
    private transient ValueState<EmaState> state;
    private transient LongAdder windows, buy, sell, initial;
    private transient IssueCounts counts;
    private transient Reporter reporter;

    /** @param jumpRatio report a close that differs from the previous close by more than this ratio (0.1 = 10%) */
    public EmaFunction(double jumpRatio) {
        this.jumpRatio = jumpRatio;
    }

    @Override
    public void open(OpenContext ctx) {
        state = getRuntimeContext().getState(new ValueStateDescriptor<>("ema", TypeInformation.of(EmaState.class)));
        windows = new LongAdder();
        buy = new LongAdder();
        sell = new LongAdder();
        initial = new LongAdder();
        counts = new IssueCounts();
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        reporter = Reporter.start(() -> String.format("[ema subtask=%d] windows=%d buy=%d sell=%d (initial=%d) issues:%s",
                subtask, windows.sum(), buy.sum(), sell.sum(), initial.sum(), counts.format()));
    }

    @Override
    public void processElement(PriceWindow w, Context ctx, Collector<EmaWindow> out) throws Exception {
        EmaState s = state.value();
        if (s == null) {
            s = new EmaState();
        }
        if (!s.accepts(w)) {
            counts.add(IssueType.WINDOW_OUT_OF_ORDER);
            ctx.output(ISSUES, DataIssue.ofWindow(IssueType.WINDOW_OUT_OF_ORDER, w.symbol, w.windowEnd, w.close,
                    "window ending " + w.windowEnd + " after the window ending " + s.lastWindowEnd));
            return;
        }
        if (s.lastClose > 0 && Math.abs(w.close / s.lastClose - 1) > jumpRatio) {
            counts.add(IssueType.PRICE_JUMP);
            ctx.output(ISSUES, DataIssue.ofWindow(IssueType.PRICE_JUMP, w.symbol, w.windowEnd, w.close,
                    String.format("close %s -> %s (%+.1f%%)", s.lastClose, w.close, (w.close / s.lastClose - 1) * 100)));
        }
        EmaWindow r = s.update(w);
        r.emittedAt = System.currentTimeMillis();
        state.update(s);

        windows.increment();
        if ("BUY".equals(r.advisory)) {
            buy.increment();
        } else if ("SELL".equals(r.advisory)) {
            sell.increment();
        }
        if (r.initial) {
            initial.increment();
        }
        out.collect(r);
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }
}
