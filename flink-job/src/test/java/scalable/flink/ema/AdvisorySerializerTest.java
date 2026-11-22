package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdvisorySerializerTest {

    @Test
    void writesOneJsonObjectWithLocalMarketTimes() {
        EmaWindow r = new EmaWindow();
        r.symbol = "ASML.NL";
        r.advisory = "SELL";
        r.windowStart = MarketEventParserTest.ms("2021-11-10T09:30:00");
        r.windowEnd = MarketEventParserTest.ms("2021-11-10T09:35:00");
        r.close = 712.3;
        r.ema38 = 711.5;
        r.ema100 = 711.75;
        r.prevEma38 = 711.8;
        r.prevEma100 = 711.7;
        r.initial = false;
        r.firedAt = 1_795_111_225_749L;
        r.emittedAt = 1_795_111_225_751L;
        String json = new String(new AdvisorySerializer().serialize(new TraderAdvisory("trader-tech", r)),
                StandardCharsets.UTF_8);
        assertEquals("{\"trader\":\"trader-tech\",\"symbol\":\"ASML.NL\",\"type\":\"SELL\",\"window_start\":\"2021-11-10T09:30:00\","
                + "\"window_end\":\"2021-11-10T09:35:00\",\"due_at\":\"2021-11-10T09:35:00\",\"close\":712.3,"
                + "\"ema38\":711.5,\"ema100\":711.75,\"prev_ema38\":711.8,\"prev_ema100\":711.7,\"initial\":false,"
                + "\"fired_at\":\"2026-11-19T18:00:25.749Z\",\"emitted_at\":\"2026-11-19T18:00:25.751Z\"}", json);
    }

    @Test
    void escapesStrings() {
        StringBuilder sb = new StringBuilder();
        AdvisorySerializer.string(sb, "a\"b\\c\u0001");
        assertEquals("\"a\\\"b\\\\c\\u0001\"", sb.toString());
        sb.setLength(0);
        AdvisorySerializer.string(sb, null);
        assertEquals("null", sb.toString());
    }

    @Test
    void localTimesKeepMilliseconds() {
        assertEquals("2021-11-08T13:05:09.564", AdvisorySerializer.local(MarketEventParserTest.ms("2021-11-08T13:05:09.564")));
    }
}
