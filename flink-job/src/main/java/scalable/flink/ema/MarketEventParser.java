package scalable.flink.ema;

import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;

/**
 * Parses one DEBS 2022 CSV row (bytes, as sent by the replay producer) into a {@link MarketEvent}.
 *
 * <p>Returns null for rows that are not price events (`Last` empty): most of the input. Those rows are skipped
 * without creating any objects. Only the fields this job needs are located:
 * 0 ID, 1 SecType, 2 Date (dd-MM-yyyy), 3 Time, 21 Last, 23 Trading time (HH:mm:ss.fff), 26 Trading date.
 *
 * <p>Timestamps are local exchange time without a zone. They are read as if they were UTC, so 5-min windows
 * aligned to the epoch start at local midnight (FR-6, architecture §3.3).
 */
public final class MarketEventParser {
    static final int ID = 0, SEC_TYPE = 1, DATE = 2, TIME = 3, LAST = 21, TRADING_TIME = 23, TRADING_DATE = 26;
    private static final int FIELDS = TRADING_DATE + 1;
    private static final long DAY_MS = 86_400_000L;

    private MarketEventParser() {}

    public static MarketEvent parse(byte[] row, int partition, long offset) {
        int len = row.length;
        while (len > 0 && (row[len - 1] == '\n' || row[len - 1] == '\r')) {
            len--;
        }
        // start[i] = first byte of field i; field i ends before start[i + 1] - 1 (the comma), or at len.
        int[] start = new int[FIELDS + 1];
        int found = 1;
        for (int i = 0; i < len && found <= FIELDS; i++) {
            if (row[i] == ',') {
                start[found++] = i + 1;
            }
        }
        if (found <= LAST) {
            return null; // too short to have `Last`: not a price event
        }
        int lastStart = start[LAST], lastEnd = end(start, found, LAST, len);
        if (lastEnd <= lastStart) {
            return null; // `Last` empty: not a price event (FR-9)
        }

        MarketEvent e = new MarketEvent();
        e.partition = partition;
        e.offset = offset;
        e.symbol = str(row, start[ID], end(start, found, ID, len));
        e.secType = str(row, start[SEC_TYPE], end(start, found, SEC_TYPE, len));
        e.systemTime = Long.MIN_VALUE;

        if (e.symbol.isEmpty()) {
            return reject(e, IssueType.MALFORMED, "empty ID", row, len);
        }
        if (found <= TRADING_TIME) {
            return reject(e, IssueType.MALFORMED, "row has only " + found + " fields", row, len);
        }
        String last = str(row, lastStart, lastEnd);
        try {
            e.price = Double.parseDouble(last);
        } catch (NumberFormatException ex) {
            return reject(e, IssueType.MALFORMED, "Last not a number: " + last, row, len);
        }
        if (!Double.isFinite(e.price)) {
            return reject(e, IssueType.MALFORMED, "Last not finite: " + last, row, len);
        }

        long date = parseDate(row, start[DATE], end(start, found, DATE, len));
        long sysTime = parseTime(row, start[TIME], end(start, found, TIME, len));
        if (date != Long.MIN_VALUE && sysTime >= 0) {
            e.systemTime = date * DAY_MS + sysTime;
        }
        long tradingDate = Long.MIN_VALUE;
        if (found > TRADING_DATE && end(start, found, TRADING_DATE, len) > start[TRADING_DATE]) {
            tradingDate = parseDate(row, start[TRADING_DATE], end(start, found, TRADING_DATE, len));
            if (tradingDate == Long.MIN_VALUE) {
                return reject(e, IssueType.MALFORMED, "Trading date unparseable", row, len);
            }
            e.tradingDateDiffers = date != Long.MIN_VALUE && tradingDate != date;
        }
        long eventDate = tradingDate != Long.MIN_VALUE ? tradingDate : date;
        if (eventDate == Long.MIN_VALUE) {
            return reject(e, IssueType.MALFORMED, "Date unparseable", row, len);
        }
        long tradingTime = parseTime(row, start[TRADING_TIME], end(start, found, TRADING_TIME, len));
        if (tradingTime < 0) {
            return reject(e, IssueType.MALFORMED, "Trading time missing or unparseable", row, len);
        }
        e.eventTime = eventDate * DAY_MS + tradingTime;

        if (date != Long.MIN_VALUE && Math.abs(eventDate - date) > 1) {
            return reject(e, IssueType.IMPLAUSIBLE_TIME, "event date " + (eventDate - date) + " days from Date", row, len);
        }
        e.timeValid = true;
        if (e.price == 0) {
            e.reject = IssueType.ZERO_PRICE.name(); // common (daily reset records): no detail string
            return e;
        }
        if (e.price < 0) {
            return reject(e, IssueType.NEGATIVE_PRICE, "Last " + last, row, len);
        }
        return e;
    }

    private static MarketEvent reject(MarketEvent e, IssueType type, String detail, byte[] row, int len) {
        e.reject = type.name();
        e.rejectDetail = detail + " | row: " + str(row, 0, Math.min(len, 200));
        return e;
    }

    private static int end(int[] start, int found, int field, int len) {
        return field + 1 < found ? start[field + 1] - 1 : len;
    }

    private static String str(byte[] b, int from, int to) {
        return to > from ? new String(b, from, to - from, StandardCharsets.UTF_8) : "";
    }

    /** "dd-MM-yyyy" → epoch day, or {@link Long#MIN_VALUE}. */
    static long parseDate(byte[] b, int s, int e) {
        if (e - s != 10 || b[s + 2] != '-' || b[s + 5] != '-') {
            return Long.MIN_VALUE;
        }
        int d = digits(b, s, 2), m = digits(b, s + 3, 2), y = digits(b, s + 6, 4);
        if (d < 0 || m < 0 || y < 0) {
            return Long.MIN_VALUE;
        }
        try {
            return LocalDate.of(y, m, d).toEpochDay();
        } catch (DateTimeException ex) {
            return Long.MIN_VALUE;
        }
    }

    /** "HH:mm:ss" with an optional fraction (".f", ".fff", ...) → ms of day, or -1. */
    static long parseTime(byte[] b, int s, int e) {
        if (e - s < 8 || b[s + 2] != ':' || b[s + 5] != ':') {
            return -1;
        }
        int h = digits(b, s, 2), mi = digits(b, s + 3, 2), sec = digits(b, s + 6, 2);
        if (h < 0 || h > 23 || mi < 0 || mi > 59 || sec < 0 || sec > 59) {
            return -1;
        }
        int ms = 0;
        if (e - s > 8) {
            if (b[s + 8] != '.' || e - s == 9) {
                return -1;
            }
            int scale = 100;
            for (int i = s + 9; i < e; i++) {
                int dgt = b[i] - '0';
                if (dgt < 0 || dgt > 9) {
                    return -1;
                }
                ms += dgt * scale; // digits beyond milliseconds add 0 (scale reaches 0)
                scale /= 10;
            }
        }
        return h * 3_600_000L + mi * 60_000L + sec * 1_000L + ms;
    }

    private static int digits(byte[] b, int s, int n) {
        int v = 0;
        for (int i = s; i < s + n; i++) {
            int dgt = b[i] - '0';
            if (dgt < 0 || dgt > 9) {
                return -1;
            }
            v = v * 10 + dgt;
        }
        return v;
    }
}
