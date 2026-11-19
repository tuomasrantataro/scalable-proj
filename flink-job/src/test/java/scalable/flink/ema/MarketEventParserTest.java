package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MarketEventParserTest {

    /** A CSV row of the DEBS format: 39 columns plus the trailing comma of the real files. */
    static byte[] row(String id, String date, String time, String last, String tradingTime, String tradingDate) {
        String[] f = new String[40];
        java.util.Arrays.fill(f, "");
        f[0] = id;
        f[1] = "E";
        f[2] = date;
        f[3] = time;
        f[21] = last;
        f[23] = tradingTime;
        f[26] = tradingDate;
        return String.join(",", f).getBytes(StandardCharsets.UTF_8);
    }

    static long ms(String isoLocal) {
        return LocalDateTime.parse(isoLocal).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    static MarketEvent parse(byte[] b) {
        return MarketEventParser.parse(b, 0, 42);
    }

    @Test
    void nonPriceRowsAreSkipped() {
        assertNull(parse(row("A.FR", "08-11-2021", "10:00:00.000", "", "", "")));
        assertNull(parse("A.FR,E,08-11-2021,10:00:00.000".getBytes(StandardCharsets.UTF_8)));
        assertNull(parse(new byte[0]));
    }

    @Test
    void priceEventUsesDateAndTradingTime() {
        MarketEvent e = parse(row("RDSA.NL", "08-11-2021", "12:00:00.000", "21.5", "13:05:09.564", ""));
        assertNotNull(e);
        assertTrue(e.usable());
        assertEquals("RDSA.NL", e.symbol);
        assertEquals("E", e.secType);
        assertEquals(21.5, e.price);
        assertEquals(ms("2021-11-08T13:05:09.564"), e.eventTime);
        assertEquals(ms("2021-11-08T12:00:00"), e.systemTime);
        assertEquals(42, e.offset);
        assertFalse(e.tradingDateDiffers);
        assertTrue(e.timeValid);
    }

    @Test
    void tradingDateTakesPrecedenceAndMismatchIsFlagged() {
        MarketEvent e = parse(row("A.FR", "09-11-2021", "00:00:05.000", "1.0", "23:59:58.000", "08-11-2021"));
        assertTrue(e.usable());
        assertEquals(ms("2021-11-08T23:59:58"), e.eventTime);
        assertTrue(e.tradingDateDiffers);
        MarketEvent same = parse(row("A.FR", "09-11-2021", "10:00:00.000", "1.0", "10:00:00.000", "09-11-2021"));
        assertFalse(same.tradingDateDiffers);
    }

    @Test
    void lineEndingsAndFractionsAreHandled() {
        byte[] crlf = (new String(row("A.FR", "08-11-2021", "10:00:00", "2", "10:00:01", ""), StandardCharsets.UTF_8)
                + "\r\n").getBytes(StandardCharsets.UTF_8);
        assertEquals(ms("2021-11-08T10:00:01"), parse(crlf).eventTime);
        assertEquals(ms("2021-11-08T10:00:01.500"), parse(row("A.FR", "08-11-2021", "10:00:00", "2", "10:00:01.5", "")).eventTime);
        assertEquals(ms("2021-11-08T10:00:01.123"), parse(row("A.FR", "08-11-2021", "10:00:00", "2", "10:00:01.123456", "")).eventTime);
    }

    @Test
    void lastIsTheLastNeededFieldOfAShortRow() {
        // a row cut after `Trading time` is still a complete price event
        MarketEvent e = parse(String.join(",", "A.FR", "I", "08-11-2021", "10:00:00.000", "", "", "", "", "", "", "",
                "", "", "", "", "", "", "", "", "", "", "3.5", "", "10:00:02.000").getBytes(StandardCharsets.UTF_8));
        assertTrue(e.usable());
        assertEquals(3.5, e.price);
    }

    @Test
    void zeroPriceIsRejectedWithoutDetail() {
        MarketEvent e = parse(row("A.FR", "08-11-2021", "05:10:04.000", "0.000000", "00:00:00.000", ""));
        assertEquals("ZERO_PRICE", e.reject);
        assertNull(e.rejectDetail);
        assertTrue(e.timeValid);   // still advances the watermark
    }

    @Test
    void suspiciousRowsAreRejectedWithReason() {
        assertEquals("NEGATIVE_PRICE", parse(row("A.FR", "08-11-2021", "10:00:00.000", "-1.5", "10:00:00.000", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "08-11-2021", "10:00:00.000", "abc", "10:00:00.000", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "08-11-2021", "10:00:00.000", "NaN", "10:00:00.000", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "08-11-2021", "10:00:00.000", "1.0", "", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "08-11-2021", "10:00:00.000", "1.0", "25:00:00.000", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "31-02-2021", "10:00:00.000", "1.0", "10:00:00.000", "")).reject);
        assertEquals("MALFORMED", parse(row("A.FR", "08-11-2021", "10:00:00.000", "1.0", "10:00:00.000", "8-11-2021")).reject);
        assertEquals("MALFORMED", parse(row("", "08-11-2021", "10:00:00.000", "1.0", "10:00:00.000", "")).reject);
        MarketEvent far = parse(row("A.FR", "08-11-2021", "10:00:00.000", "1.0", "10:00:00.000", "08-11-2031"));
        assertEquals("IMPLAUSIBLE_TIME", far.reject);
        assertFalse(far.timeValid);
        assertTrue(far.rejectDetail.contains("row: A.FR"));
    }

    @Test
    void unparseableSystemTimeDoesNotRejectTheEvent() {
        MarketEvent e = parse(row("A.FR", "08-11-2021", "", "1.0", "10:00:00.000", ""));
        assertTrue(e.usable());
        assertEquals(Long.MIN_VALUE, e.systemTime);
    }
}
