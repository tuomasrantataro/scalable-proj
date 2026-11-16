package scalable.flink;

/** One CSV row plus where it came from in Iggy. Flink POJO (public fields, no-arg constructor). */
public class Tick {
    public String symbol;
    public String secType;
    public int partition;
    public long offset;
    /** Set by NodeCounter: which source subtask / TaskManager host read this item. */
    public int readBySubtask = -1;
    public String readByHost = "";

    public Tick() {}

    public Tick(String symbol, String secType, int partition, long offset) {
        this.symbol = symbol;
        this.secType = secType;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return symbol + "@p" + partition + "/o" + offset;
    }
}
