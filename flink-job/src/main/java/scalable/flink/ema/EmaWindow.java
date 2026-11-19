package scalable.flink.ema;

/** Query 1 + 2 result for one symbol and window. Flink POJO; stored in table `ema_window`. */
public class EmaWindow {
    public String symbol;
    public String secType;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long closeTime;
    public long ticks;
    public double ema38;
    public double ema100;
    /** EMAs after the symbol's previous non-empty window (0 before its first window, FR-12). */
    public double prevEma38;
    public double prevEma100;
    /** "BUY", "SELL" or null (FR-15, FR-16). Due at {@link #windowEnd}, when the next window starts (FR-8). */
    public String advisory;
    /** True if the previous EMAs were still at their start value 0: the advisory is an initialization artifact. */
    public boolean initial;
    /** Processing time (epoch ms) when the EMA function emitted this result. */
    public long emittedAt;

    public EmaWindow() {}
}
