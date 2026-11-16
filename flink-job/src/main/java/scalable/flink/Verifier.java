package scalable.flink;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Parallelism 1: sees every item, so it can detect duplicates across nodes
 * (same partition+offset delivered twice) and show which node read which partition.
 * Offsets are kept in a BitSet per partition (about 1 bit per message).
 */
public class Verifier extends RichMapFunction<Tick, Tick> {

    private transient long total;
    private transient long duplicates;
    private transient Map<Integer, BitSet> offsets;
    private transient Map<Integer, Set<String>> readers;
    private transient Reporter reporter;

    @Override
    public void open(OpenContext ctx) {
        offsets = new HashMap<>();
        readers = new HashMap<>();
        reporter = Reporter.start(this::summary);
    }

    @Override
    public Tick map(Tick t) {
        synchronized (this) {
            total++;
            BitSet bits = offsets.computeIfAbsent(t.partition, k -> new BitSet());
            if (bits.get((int) t.offset)) {
                duplicates++;
            } else {
                bits.set((int) t.offset);
            }
            readers.computeIfAbsent(t.partition, k -> new TreeSet<>())
                    .add("subtask" + t.readBySubtask + "@" + t.readByHost);
        }
        return t;
    }

    private synchronized String summary() {
        long distinct = offsets.values().stream().mapToLong(BitSet::cardinality).sum();
        StringBuilder sb = new StringBuilder(String.format(
                "[verifier] total=%d distinct=%d duplicates=%d", total, distinct, duplicates));
        new TreeMap<>(offsets).forEach((p, bits) -> {
            int first = bits.nextSetBit(0);
            int last = bits.length() - 1;
            long expected = first < 0 ? 0 : last - first + 1;
            sb.append(String.format("%n    partition %d: distinct=%d offsets=[%d..%d] gaps=%d readBy=%s",
                    p, bits.cardinality(), first, last, expected - bits.cardinality(),
                    readers.getOrDefault(p, Set.of())));
        });
        return sb.toString();
    }

    @Override
    public void close() {
        if (reporter != null) {
            reporter.stop();
        }
    }
}
