package scalable.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Runs after keyBy(symbol): exercises the network shuffle between the TaskManagers and keyed state.
 * Keeps a per-symbol count in keyed state and logs per-node totals.
 */
public class SymbolCounter extends RichMapFunction<Tick, Long> {

    private transient ValueState<Long> count;
    private transient LongAdder total;
    private transient LongAdder fromOtherNode;
    private transient Set<String> symbols;
    private transient Reporter reporter;

    @Override
    public void open(OpenContext ctx) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        String host = Reporter.hostname();
        total = new LongAdder();
        fromOtherNode = new LongAdder();
        symbols = ConcurrentHashMap.newKeySet();
        reporter = Reporter.start(() -> String.format(
                "[symbol-counter subtask=%d host=%s] total=%d distinctSymbols=%d receivedFromOtherHost=%d",
                subtask, host, total.sum(), symbols.size(), fromOtherNode.sum()));
        this.host = host;
    }

    private transient String host;

    @Override
    public Long map(Tick t) throws Exception {
        Long c = count.value();
        c = c == null ? 1L : c + 1;
        count.update(c);
        total.increment();
        symbols.add(t.symbol);
        if (!host.equals(t.readByHost)) {
            fromOtherNode.increment();
        }
        return c;
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }
}
