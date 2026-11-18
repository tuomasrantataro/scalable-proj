package scalable.flink;

/** Result of one 5-minute window for one symbol: the last price by event time. Flink POJO. */
public class WindowClose {
    public String symbol;
    public long windowStart;
    public long windowEnd;
    public double close;
    public long ticks;
    public long lastEventTime;

    public WindowClose() {}
}
