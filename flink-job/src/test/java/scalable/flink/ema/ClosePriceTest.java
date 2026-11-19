package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClosePriceTest {

    static MarketEvent ev(long time, long offset, double price) {
        MarketEvent e = new MarketEvent();
        e.symbol = "A.FR";
        e.secType = "E";
        e.eventTime = time;
        e.offset = offset;
        e.price = price;
        return e;
    }

    @Test
    void closeIsLatestEventTimeNotLastArrival() {
        ClosePrice.Aggregate agg = new ClosePrice.Aggregate();
        ClosePrice.Acc a = agg.createAccumulator();
        a = agg.add(ev(2_000, 1, 2.0), a);
        a = agg.add(ev(1_000, 2, 1.0), a);   // arrives later, happened earlier
        assertEquals(2.0, a.price);
        assertEquals(2_000, a.time);
        assertEquals(2, a.ticks);
    }

    @Test
    void equalEventTimesGoToTheLaterOffset() {
        ClosePrice.Aggregate agg = new ClosePrice.Aggregate();
        ClosePrice.Acc a = agg.createAccumulator();
        a = agg.add(ev(1_000, 7, 1.0), a);
        a = agg.add(ev(1_000, 8, 3.0), a);
        a = agg.add(ev(1_000, 6, 2.0), a);
        assertEquals(3.0, a.price);
    }

    @Test
    void mergeKeepsTheLaterCloseAndSumsTicks() {
        ClosePrice.Aggregate agg = new ClosePrice.Aggregate();
        ClosePrice.Acc a = agg.add(ev(5_000, 1, 5.0), agg.createAccumulator());
        ClosePrice.Acc b = agg.add(ev(3_000, 9, 3.0), agg.add(ev(4_000, 8, 4.0), agg.createAccumulator()));
        ClosePrice.Acc m = agg.merge(b, a);
        assertEquals(5.0, m.price);
        assertEquals(3, m.ticks);
        assertEquals(5.0, agg.merge(a, b).price);
    }
}
