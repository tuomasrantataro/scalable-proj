package scalable.flink.ema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Kafka record (value = one CSV row) → {@link MarketEvent}; non-price rows emit nothing. */
public class MarketEventDeserializer implements KafkaRecordDeserializationSchema<MarketEvent> {
    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MarketEvent> out) {
        MarketEvent e = MarketEventParser.parse(record.value(), record.partition(), record.offset());
        if (e != null) {
            out.collect(e);
        }
    }

    @Override
    public TypeInformation<MarketEvent> getProducedType() {
        return TypeInformation.of(MarketEvent.class);
    }
}
