package scalable.flink.ema;

/**
 * Per-symbol EMA state and the Query 1 / Query 2 calculation. Plain Java (no Flink types), so it can be
 * unit-tested; {@link EmaFunction} keeps one instance per symbol in keyed state. Flink POJO.
 *
 * <pre>
 *   EMA_j(w_i) = Close(w_i) · 2/(1+j) + EMA_j(w_{i-1}) · (1 − 2/(1+j))      j ∈ {38, 100}   (FR-11)
 *   EMA_j before the first window = 0                                                    (FR-12)
 *   BUY  if EMA38(w_i) > EMA100(w_i) and EMA38(w_{i-1}) ≤ EMA100(w_{i-1})               (FR-15)
 *   SELL if EMA38(w_i) < EMA100(w_i) and EMA38(w_{i-1}) ≥ EMA100(w_{i-1})               (FR-16)
 * </pre>
 *
 * Only windows with a price event update the state; an empty window leaves the EMAs unchanged and is not
 * evaluated (architecture §3.5), so w_{i-1} is the symbol's previous non-empty window.
 */
public class EmaState {
    public static final double ALPHA_38 = 2.0 / (1 + 38);
    public static final double ALPHA_100 = 2.0 / (1 + 100);

    public double ema38;
    public double ema100;
    /** Window end of the last update, epoch ms; {@link Long#MIN_VALUE} before the first. */
    public long lastWindowEnd = Long.MIN_VALUE;
    /** Close of the last window, for spotting price jumps; NaN before the first. */
    public double lastClose = Double.NaN;
    public long windows;

    public EmaState() {}

    /** Applies one window. The caller must check {@link #accepts} first. */
    public EmaWindow update(PriceWindow w) {
        EmaWindow out = new EmaWindow();
        out.symbol = w.symbol;
        out.secType = w.secType;
        out.windowStart = w.windowStart;
        out.windowEnd = w.windowEnd;
        out.close = w.close;
        out.closeTime = w.closeTime;
        out.ticks = w.ticks;
        out.prevEma38 = ema38;
        out.prevEma100 = ema100;

        ema38 = next(ema38, w.close, ALPHA_38);
        ema100 = next(ema100, w.close, ALPHA_100);
        out.ema38 = ema38;
        out.ema100 = ema100;
        out.advisory = crossover(out.prevEma38, out.prevEma100, ema38, ema100);
        out.initial = out.advisory != null && out.prevEma38 == 0 && out.prevEma100 == 0;

        lastWindowEnd = w.windowEnd;
        lastClose = w.close;
        windows++;
        return out;
    }

    /** Windows of one symbol must arrive in window order; an older or repeated window is refused. */
    public boolean accepts(PriceWindow w) {
        return w.windowEnd > lastWindowEnd;
    }

    /** FR-11, evaluated literally: close · α + previous · (1 − α). */
    static double next(double previous, double close, double alpha) {
        return close * alpha + previous * (1 - alpha);
    }

    /** FR-15 / FR-16. */
    static String crossover(double prev38, double prev100, double ema38, double ema100) {
        if (ema38 > ema100 && prev38 <= prev100) {
            return "BUY";
        }
        if (ema38 < ema100 && prev38 >= prev100) {
            return "SELL";
        }
        return null;
    }
}
