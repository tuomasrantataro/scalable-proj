package scalable.flink.ema;

/**
 * A price event (CSV row with `Last` set), parsed by {@link MarketEventParser}. Flink POJO.
 *
 * <p>Usable for the EMA calculation only if {@link #reject} is null. Rejected events are still emitted so that
 * the quality gate can count and report them.
 */
public class MarketEvent {
    public String symbol;
    public String secType;
    /** Event time, epoch ms: (`Trading date`, or `Date` when empty) + `Trading time`, local time read as UTC. */
    public long eventTime;
    /** System time of the row, epoch ms: `Date` + `Time`; {@link Long#MIN_VALUE} if it does not parse. */
    public long systemTime;
    public double price;
    public int partition;
    public long offset;
    /** Why the event cannot be used ({@link IssueType} name), or null. */
    public String reject;
    public String rejectDetail;
    /**
     * {@link #eventTime} parsed and is plausible, so the event may advance the watermark even if it is rejected
     * for another reason (e.g. a zero price still shows that time has moved on).
     */
    public boolean timeValid;
    /** `Trading date` is set and differs from `Date` (kept, reported). */
    public boolean tradingDateDiffers;

    public MarketEvent() {}

    public boolean usable() {
        return reject == null;
    }
}
