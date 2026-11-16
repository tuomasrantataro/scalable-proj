package scalable.flink;

import org.apache.iggy.connector.serialization.DeserializationSchema;
import org.apache.iggy.connector.serialization.RecordMetadata;
import org.apache.iggy.connector.serialization.TypeDescriptor;

import java.nio.charset.StandardCharsets;

/** Parses the CSV line just enough for the test, and keeps the Iggy partition/offset for verification. */
public class TickDeserializer implements DeserializationSchema<Tick> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tick deserialize(byte[] data, RecordMetadata meta) {
        String line = new String(data, StandardCharsets.UTF_8);
        String[] f = line.split(",", 3);
        return new Tick(
                f[0],
                f.length > 1 ? f[1] : "",
                meta.getPartitionId() == null ? -1 : meta.getPartitionId(),
                meta.getOffset() == null ? -1 : meta.getOffset());
    }

    @Override
    public TypeDescriptor<Tick> getProducedType() {
        return TypeDescriptor.of(Tick.class);
    }
}
