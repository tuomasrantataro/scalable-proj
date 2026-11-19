package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmaStateTest {
    static final long W = 300_000L;

    static PriceWindow window(int i, double close) {
        PriceWindow w = new PriceWindow();
        w.symbol = "A.FR";
        w.secType = "E";
        w.windowStart = i * W;
        w.windowEnd = (i + 1) * W;
        w.close = close;
        w.ticks = 1;
        return w;
    }

    @Test
    void firstWindowStartsFromZeroAndGivesAnInitialBuy() {
        EmaState s = new EmaState();
        EmaWindow r = s.update(window(0, 100.0));
        assertEquals(0.0, r.prevEma38);
        assertEquals(0.0, r.prevEma100);
        assertEquals(100.0 * (2.0 / 39), r.ema38);
        assertEquals(100.0 * (2.0 / 101), r.ema100);
        assertEquals("BUY", r.advisory);
        assertTrue(r.initial);
    }

    @Test
    void recursionFollowsFr11() {
        double[] closes = {10, 20, 15, 30, 30, 5};
        EmaState s = new EmaState();
        double e38 = 0, e100 = 0;
        for (int i = 0; i < closes.length; i++) {
            EmaWindow r = s.update(window(i, closes[i]));
            assertEquals(e38, r.prevEma38);
            assertEquals(e100, r.prevEma100);
            e38 = closes[i] * (2.0 / (1 + 38)) + e38 * (1 - 2.0 / (1 + 38));
            e100 = closes[i] * (2.0 / (1 + 100)) + e100 * (1 - 2.0 / (1 + 100));
            assertEquals(e38, r.ema38, 0.0);
            assertEquals(e100, r.ema100, 0.0);
        }
        assertEquals(closes.length, s.windows);
        assertEquals(5.0, s.lastClose);
    }

    @Test
    void crossoversAfterWarmUp() {
        // Constant 100 for 60 windows, a crash to 1, then a rally to 1000. EMA38 reacts faster than EMA100,
        // so it falls below (SELL) and later rises above (BUY). Expected windows from the FR-11 recursion.
        List<String> got = new ArrayList<>();
        List<Integer> at = new ArrayList<>();
        EmaState s = new EmaState();
        for (int i = 0; i < 120; i++) {
            double close = i < 60 ? 100 : i < 90 ? 1 : 1000;
            EmaWindow r = s.update(window(i, close));
            if (r.advisory != null) {
                got.add(r.advisory + (r.initial ? "*" : ""));
                at.add(i);
            }
        }
        assertEquals(List.of("BUY*", "SELL", "BUY"), got);
        assertEquals(List.of(0, 69, 90), at);
    }

    @Test
    void equalEmasDoNotTriggerButLeavingEqualityDoes() {
        assertNull(EmaState.crossover(1, 1, 1, 1));
        assertEquals("BUY", EmaState.crossover(1, 1, 2, 1));   // prev38 <= prev100
        assertEquals("SELL", EmaState.crossover(1, 1, 1, 2));  // prev38 >= prev100
        assertNull(EmaState.crossover(2, 1, 3, 1));            // already above
        assertNull(EmaState.crossover(1, 2, 1, 3));            // already below
    }

    @Test
    void olderOrRepeatedWindowsAreRefused() {
        EmaState s = new EmaState();
        assertTrue(s.accepts(window(5, 1)));
        s.update(window(5, 1));
        assertFalse(s.accepts(window(5, 1)));
        assertFalse(s.accepts(window(4, 1)));
        assertTrue(s.accepts(window(9, 1)));   // gaps (empty windows) are fine
    }
}
