package scalable.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.config.OffsetConfig;
import org.apache.iggy.connector.flink.source.IggySource;
import org.apache.iggy.connector.serialization.DeserializationSchema;
import scalable.flink.ema.EmaPipeline;

/**
 * Connector smoke test: Iggy source (parallel) → per-node counter → keyBy(symbol) per-node counter,
 * plus a single global verifier that checks for lost / duplicated messages.
 *
 * <pre>
 *   IggySource (p=N) ─► NodeCounter (p=N, chained) ─┬─► keyBy(symbol) ─► SymbolCounter (p=N) ─► discard
 *                                                     └─► Verifier (p=1) ─► discard
 * </pre>
 */
public final class ConnectorTestJob {

    private ConnectorTestJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String pipeline = envOr("PIPELINE", "full");

        // PIPELINE=ema: EMA38/EMA100 per 5-min window, crossover advisories, data quality report (TimescaleDB).
        if ("ema".equals(pipeline)) {
            EmaPipeline.build(env);
            env.execute("trading-trends-ema");
            return;
        }

        // PIPELINE=jdbc: price events -> 5-min windows -> TimescaleDB via the Flink JDBC connector.
        if ("jdbc".equals(pipeline)) {
            JdbcSinkPipeline.build(env, source(new PriceDeserializer()));
            env.execute("jdbc-sink-test");
            return;
        }

        IggySource<Tick> source = source(new TickDeserializer());

        DataStream<Tick> ticks = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "iggy-source", TypeInformation.of(Tick.class))
                .map(new NodeCounter()).name("node-counter");

        // PIPELINE=count: source -> node-counter -> discard only, to measure the source on its own.
        if ("count".equals(pipeline)) {
            ticks.sinkTo(new DiscardingSink<>()).name("discard");
        } else {
            ticks.keyBy(t -> t.symbol)
                    .map(new SymbolCounter()).name("symbol-counter")
                    .sinkTo(new DiscardingSink<>()).name("discard-keyed");

            ticks.map(new Verifier()).name("verifier").setParallelism(1)
                    .sinkTo(new DiscardingSink<>()).name("discard-verifier").setParallelism(1);
        }

        env.execute("iggy-connector-test");
    }

    public static <T> IggySource<T> source(DeserializationSchema<T> deserializer) {
        return IggySource.<T>builder()
                .setConnectionConfig(iggyConnection())
                .setStreamId(envOr("IGGY_STREAM", "trading"))
                .setTopicId(envOr("IGGY_TOPIC", "ticks"))
                .setConsumerGroup(envOr("IGGY_CONSUMER_GROUP", "flink"))
                .setOffsetConfig(OffsetConfig.builder()
                        .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                        .build())
                .setPollBatchSize(Long.parseLong(envOr("IGGY_POLL_BATCH", "100")))
                .setDeserializer(deserializer)
                .build();
    }

    /** Iggy server and credentials, shared by the source and the advisory sink. */
    public static IggyConnectionConfig iggyConnection() {
        return IggyConnectionConfig.builder()
                .serverAddress(envOr("IGGY_ADDRESS", "iggy:8090"))
                .username(envOr("IGGY_USERNAME", "iggy"))
                .password(envOr("IGGY_PASSWORD", "iggy"))
                .build();
    }

    public static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
