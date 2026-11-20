package scalable.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import scalable.flink.ema.EmaPipeline;

import java.time.Duration;

/**
 * Flink entry point. {@code PIPELINE} picks the job: {@code ema} (default: EMA, advisories, data quality report)
 * or {@code jdbc} (5-min closes into TimescaleDB via the Flink JDBC connector).
 */
public final class TradingJob {

    private TradingJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String pipeline = envOr("PIPELINE", "ema");
        switch (pipeline) {
            case "ema" -> {
                EmaPipeline.build(env);
                env.execute("trading-trends-ema");
            }
            case "jdbc" -> {
                JdbcSinkPipeline.build(env, source(new PriceDeserializer()));
                env.execute("jdbc-sink-test");
            }
            default -> throw new IllegalArgumentException("unknown PIPELINE: " + pipeline + " (expected ema or jdbc)");
        }
    }

    /**
     * Kafka source for topic KAFKA_TOPIC (ticks), consumer group KAFKA_GROUP (flink). Each partition is a split
     * with its own watermark; splits are spread over all source subtasks. Starts from the group's committed
     * offsets, or from the earliest offset for a new group. Offsets are stored in Flink checkpoints (and
     * committed to Kafka on checkpoint completion, for monitoring), so a restore re-reads from the checkpoint.
     */
    public static <T> KafkaSource<T> source(KafkaRecordDeserializationSchema<T> deserializer) {
        return KafkaSource.<T>builder()
                .setBootstrapServers(bootstrapServers())
                .setTopics(envOr("KAFKA_TOPIC", "ticks"))
                .setGroupId(envOr("KAFKA_GROUP", "flink"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(deserializer)
                // larger fetches than the defaults (500 records / 50 MB): rows are ~90 B
                .setProperty("max.poll.records", envOr("KAFKA_MAX_POLL_RECORDS", "5000"))
                .setProperty("fetch.min.bytes", "65536")
                .setProperty("max.partition.fetch.bytes", "4194304")
                .build();
    }

    /**
     * Idleness (IDLENESS_S, default 10, 0 = off): a partition without records for that long stops holding back
     * the watermark, so the last windows fire when the input stops.
     *
     * <p>Watermark alignment (WATERMARK_ALIGNMENT_S, default 300, 0 = off): a partition whose watermark gets more
     * than that much event time ahead of the slowest one is paused (checked every second). The watermark is the
     * minimum over all partitions, so without alignment a lagging partition holds it back while the others read
     * on, and whether an event is late then depends on read timing. That happened when the job read a stored
     * backlog (MEASUREMENTS.md §5): runs counted up to 1,287 fewer late events than the reference.
     */
    public static <T> WatermarkStrategy<T> withIdlenessAndAlignment(WatermarkStrategy<T> wm) {
        long idlenessS = Long.parseLong(envOr("IDLENESS_S", "10"));
        long alignmentS = Long.parseLong(envOr("WATERMARK_ALIGNMENT_S", "300"));
        if (idlenessS > 0) {
            wm = wm.withIdleness(Duration.ofSeconds(idlenessS));
        }
        if (alignmentS > 0) {
            wm = wm.withWatermarkAlignment("ticks", Duration.ofSeconds(alignmentS), Duration.ofSeconds(1));
        }
        return wm;
    }

    public static String bootstrapServers() {
        return envOr("KAFKA_BOOTSTRAP", "kafka:9092");
    }

    public static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
