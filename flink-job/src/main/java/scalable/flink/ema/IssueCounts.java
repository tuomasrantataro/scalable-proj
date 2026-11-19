package scalable.flink.ema;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** Per-subtask counts by issue type, for the periodic log line. */
final class IssueCounts {
    private final Map<IssueType, LongAdder> counts = new EnumMap<>(IssueType.class);

    IssueCounts() {
        for (IssueType t : IssueType.values()) {
            counts.put(t, new LongAdder());
        }
    }

    void add(IssueType t) {
        counts.get(t).increment();
    }

    /** "zero_price=12 late=3", only types seen so far. */
    String format() {
        StringBuilder sb = new StringBuilder();
        counts.forEach((t, n) -> {
            long v = n.sum();
            if (v > 0) {
                sb.append(' ').append(t.name().toLowerCase()).append('=').append(v);
            }
        });
        return sb.length() == 0 ? " none" : sb.toString();
    }
}
