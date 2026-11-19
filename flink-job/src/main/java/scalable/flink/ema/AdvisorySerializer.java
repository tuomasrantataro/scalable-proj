package scalable.flink.ema;

import org.apache.iggy.connector.serialization.SerializationSchema;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Advisory ({@link EmaWindow} with a BUY/SELL) → one JSON message for the Iggy topic `advisories`:
 *
 * <pre>
 * {"symbol":"ASML.NL","type":"BUY","window_start":"2021-11-10T09:30:00","window_end":"2021-11-10T09:35:00",
 *  "due_at":"2021-11-10T09:35:00","close":712.3,"ema38":711.9,"ema100":711.8,"prev_ema38":711.8,
 *  "prev_ema100":711.8,"initial":false,"emitted_at":"2026-11-19T18:22:31.123Z"}
 * </pre>
 *
 * Market times are local exchange time without a zone (as in the data); `emitted_at` is a UTC instant.
 * (symbol, window_end) identifies an advisory: delivery is at-least-once, so consumers deduplicate on it.
 * Written by hand (fixed fields) to avoid depending on the connector's Jackson version.
 */
public class AdvisorySerializer implements SerializationSchema<EmaWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public byte[] serialize(EmaWindow r) {
        StringBuilder sb = new StringBuilder(320);
        sb.append("{\"symbol\":");
        string(sb, r.symbol);
        sb.append(",\"type\":");
        string(sb, r.advisory);
        sb.append(",\"window_start\":\"").append(local(r.windowStart))
          .append("\",\"window_end\":\"").append(local(r.windowEnd))
          .append("\",\"due_at\":\"").append(local(r.windowEnd))
          .append("\",\"close\":").append(r.close)
          .append(",\"ema38\":").append(r.ema38)
          .append(",\"ema100\":").append(r.ema100)
          .append(",\"prev_ema38\":").append(r.prevEma38)
          .append(",\"prev_ema100\":").append(r.prevEma100)
          .append(",\"initial\":").append(r.initial)
          .append(",\"emitted_at\":\"").append(Instant.ofEpochMilli(r.emittedAt)).append("\"}");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static final DateTimeFormatter SECONDS = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

    /** Epoch ms of local time read as UTC → "2021-11-10T09:35:00", plus ".SSS" if there are milliseconds. */
    static String local(long epochMs) {
        int ms = (int) Math.floorMod(epochMs, 1000L);
        String s = LocalDateTime.ofEpochSecond(Math.floorDiv(epochMs, 1000L), 0, ZoneOffset.UTC).format(SECONDS);
        return ms == 0 ? s : s + String.format(".%03d", ms);
    }

    /** JSON string literal; doubles are written with Double.toString, which JSON accepts for finite values. */
    static void string(StringBuilder sb, String s) {
        if (s == null) {
            sb.append("null");
            return;
        }
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }
}
