package scalable.flink.ema;

/** One advisory delivered to one subscribed trader. Flink POJO. */
public class TraderAdvisory {
    public String trader;
    public EmaWindow advisory;

    public TraderAdvisory() {}

    public TraderAdvisory(String trader, EmaWindow advisory) {
        this.trader = trader;
        this.advisory = advisory;
    }
}
