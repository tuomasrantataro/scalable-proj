package scalable.flink.ema;

/** One symbol's closed 5-min window (FR-10): the close is the price of the event with the latest event time. */
public class PriceWindow {
    public String symbol;
    public String secType;
    public long windowStart;
    public long windowEnd;
    public double close;
    /** Event time of the closing event. */
    public long closeTime;
    /** Number of usable price events in the window. */
    public long ticks;
    /** Processing time (epoch ms) when the window fired, i.e. when the watermark passed its end. */
    public long firedAt;

    public PriceWindow() {}
}
