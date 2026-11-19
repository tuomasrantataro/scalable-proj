package scalable.flink.ema;

import org.apache.iggy.connector.serialization.DeserializationSchema;
import org.apache.iggy.connector.serialization.RecordMetadata;
import org.apache.iggy.connector.serialization.TypeDescriptor;

/** Iggy message (one CSV row) → {@link MarketEvent}; null for non-price rows, which the connector skips. */
public class MarketEventDeserializer implements DeserializationSchema<MarketEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public MarketEvent deserialize(byte[] data, RecordMetadata meta) {
        return MarketEventParser.parse(data,
                meta.getPartitionId() == null ? -1 : meta.getPartitionId(),
                meta.getOffset() == null ? -1 : meta.getOffset());
    }

    @Override
    public TypeDescriptor<MarketEvent> getProducedType() {
        return TypeDescriptor.of(MarketEvent.class);
    }
}
