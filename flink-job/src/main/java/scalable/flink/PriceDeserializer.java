package scalable.flink;

import org.apache.iggy.connector.serialization.DeserializationSchema;
import org.apache.iggy.connector.serialization.RecordMetadata;
import org.apache.iggy.connector.serialization.TypeDescriptor;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

/**
 * Keeps price events only (`Last` set) and returns null for everything else; the Iggy connector skips nulls.
 * Columns (0-based): 0 ID, 2 Date (dd-MM-yyyy), 21 Last, 23 Trading time (HH:mm:ss.SSS).
 * `Trading date` (26) is empty for most rows and equals `Date` when set, so `Date` is used.
 */
public class PriceDeserializer implements DeserializationSchema<PriceTick> {
    private static final long serialVersionUID = 1L;
    private static final int ID = 0, DATE = 2, LAST = 21, TRADING_TIME = 23;

    @Override
    public PriceTick deserialize(byte[] data, RecordMetadata meta) {
        String[] f = new String(data, StandardCharsets.UTF_8).split(",", -1);
        if (f.length <= TRADING_TIME || f[LAST].isEmpty() || f[TRADING_TIME].isEmpty()) {
            return null;
        }
        return new PriceTick(
                f[ID],
                epochMillis(f[DATE], f[TRADING_TIME]),
                Double.parseDouble(f[LAST]),
                meta.getPartitionId() == null ? -1 : meta.getPartitionId(),
                meta.getOffset() == null ? -1 : meta.getOffset());
    }

    /** "08-11-2021" + "13:05:09.564" → epoch ms, treating the local exchange time as UTC. */
    static long epochMillis(String date, String time) {
        int d = Integer.parseInt(date, 0, 2, 10);
        int m = Integer.parseInt(date, 3, 5, 10);
        int y = Integer.parseInt(date, 6, 10, 10);
        long msOfDay = Integer.parseInt(time, 0, 2, 10) * 3_600_000L
                + Integer.parseInt(time, 3, 5, 10) * 60_000L
                + Integer.parseInt(time, 6, 8, 10) * 1_000L
                + (time.length() >= 12 ? Integer.parseInt(time, 9, 12, 10) : 0);
        return LocalDate.of(y, m, d).toEpochDay() * 86_400_000L + msOfDay;
    }

    @Override
    public TypeDescriptor<PriceTick> getProducedType() {
        return TypeDescriptor.of(PriceTick.class);
    }
}
