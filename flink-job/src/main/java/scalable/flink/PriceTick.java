package scalable.flink;

/** A price event (CSV row with `Last` set). Flink POJO. */
public class PriceTick {
    public String symbol;
    /** Event time in epoch ms: `Date` + `Trading time`, read as UTC (architecture §3.3). */
    public long eventTime;
    public double price;
    public int partition;
    public long offset;

    public PriceTick() {}

    public PriceTick(String symbol, long eventTime, double price, int partition, long offset) {
        this.symbol = symbol;
        this.eventTime = eventTime;
        this.price = price;
        this.partition = partition;
        this.offset = offset;
    }
}
