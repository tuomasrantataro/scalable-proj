package scalable.flink.ema;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Window aggregation for FR-10: the close is the price of the event with the latest event time, not the last
 * one to arrive, so events out of order within a window are handled. Equal event times go to the event that
 * came later in the input (higher Iggy offset), which keeps the result deterministic.
 */
public final class ClosePrice {
    private ClosePrice() {}

    /** Accumulator. Flink POJO. */
    public static class Acc {
        public long time = Long.MIN_VALUE;
        public long offset = Long.MIN_VALUE;
        public int partition = Integer.MIN_VALUE;
        public double price;
        public long ticks;
        public String secType;

        public Acc() {}

        boolean before(long t, long o, int p) {
            if (time != t) {
                return time < t;
            }
            if (offset != o) {
                return offset < o;
            }
            return partition < p;
        }
    }

    public static class Aggregate implements AggregateFunction<MarketEvent, Acc, Acc> {
        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public Acc add(MarketEvent e, Acc a) {
            a.ticks++;
            if (a.before(e.eventTime, e.offset, e.partition)) {
                a.time = e.eventTime;
                a.offset = e.offset;
                a.partition = e.partition;
                a.price = e.price;
                a.secType = e.secType;
            }
            return a;
        }

        @Override
        public Acc getResult(Acc a) {
            return a;
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            Acc later = a.before(b.time, b.offset, b.partition) ? b : a;
            Acc m = new Acc();
            m.time = later.time;
            m.offset = later.offset;
            m.partition = later.partition;
            m.price = later.price;
            m.secType = later.secType;
            m.ticks = a.ticks + b.ticks;
            return m;
        }
    }

    public static class ToPriceWindow extends ProcessWindowFunction<Acc, PriceWindow, String, TimeWindow> {
        @Override
        public void process(String symbol, Context ctx, Iterable<Acc> in, Collector<PriceWindow> out) {
            Acc a = in.iterator().next();
            PriceWindow w = new PriceWindow();
            w.symbol = symbol;
            w.secType = a.secType;
            w.windowStart = ctx.window().getStart();
            w.windowEnd = ctx.window().getEnd();
            w.close = a.price;
            w.closeTime = a.time;
            w.ticks = a.ticks;
            out.collect(w);
        }
    }
}
