package scalable.flink.ema;

/** Kinds of suspicious data. "Dropped" ones are excluded from the calculation; "kept" ones are only reported. */
public enum IssueType {
    /** `Last = 0`: reset records, not trades. Dropped and counted, not stored row by row. */
    ZERO_PRICE,
    /** `Last` is not a finite number, or `Trading time` / `Date` / `Trading date` is missing or unparseable. Dropped. */
    MALFORMED,
    /** `Last < 0`. Dropped. */
    NEGATIVE_PRICE,
    /** Event date more than a day away from the row's system date: probably a corrupt date. Dropped. */
    IMPLAUSIBLE_TIME,
    /** Arrived after its 5-min window was evaluated (watermark passed the window end). Dropped. */
    LATE,
    /** `Trading date` set and different from `Date`. Kept (the trading date is used). */
    TRADING_DATE_MISMATCH,
    /** `Trading time` differs from the system `Time` by more than the threshold (default 1 h). Kept. */
    TIME_SKEW,
    /** A window close differs from the symbol's previous close by more than the threshold (default 10%). Kept. */
    PRICE_JUMP,
    /** A symbol's window arrived at the EMA function out of window order. Dropped (should never happen). */
    WINDOW_OUT_OF_ORDER;

    public boolean dropsEvent() {
        return this == ZERO_PRICE || this == MALFORMED || this == NEGATIVE_PRICE || this == IMPLAUSIBLE_TIME
                || this == LATE || this == WINDOW_OUT_OF_ORDER;
    }
}
