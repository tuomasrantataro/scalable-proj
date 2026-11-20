package scalable.flink.ema;

/** One reported piece of suspicious data. Flink POJO; stored in table `data_issue`. */
public class DataIssue {
    public String reason;
    public String symbol;
    /** Event time (epoch ms) of the event, or the window end for window-level issues. */
    public long eventTime;
    public double price;
    /** Kafka position (partition, offset) of the event, or -1 for window-level issues. Part of the key, so replays are idempotent. */
    public int partition;
    public long offset;
    public String detail;

    public DataIssue() {}

    static DataIssue of(IssueType type, MarketEvent e, String detail) {
        DataIssue i = new DataIssue();
        i.reason = type.name();
        i.symbol = e.symbol;
        i.eventTime = e.eventTime;
        i.price = e.price;
        i.partition = e.partition;
        i.offset = e.offset;
        i.detail = detail;
        return i;
    }

    static DataIssue ofWindow(IssueType type, String symbol, long windowEnd, double price, String detail) {
        DataIssue i = new DataIssue();
        i.reason = type.name();
        i.symbol = symbol;
        i.eventTime = windowEnd;
        i.price = price;
        i.partition = -1;
        i.offset = -1;
        i.detail = detail;
        return i;
    }
}
