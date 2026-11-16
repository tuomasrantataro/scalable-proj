package scalable.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.iggy.connector.config.IggyConnectionConfig;
import org.apache.iggy.connector.config.OffsetConfig;
import org.apache.iggy.connector.flink.source.IggySource;

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

        IggySource<Tick> source = IggySource.<Tick>builder()
                .setConnectionConfig(IggyConnectionConfig.builder()
                        .serverAddress(envOr("IGGY_ADDRESS", "iggy:8090"))
                        .username(envOr("IGGY_USERNAME", "iggy"))
                        .password(envOr("IGGY_PASSWORD", "iggy"))
                        .build())
                .setStreamId(envOr("IGGY_STREAM", "trading"))
                .setTopicId(envOr("IGGY_TOPIC", "ticks"))
                .setConsumerGroup(envOr("IGGY_CONSUMER_GROUP", "flink"))
                .setOffsetConfig(OffsetConfig.builder()
                        .resetStrategy(OffsetConfig.OffsetResetStrategy.EARLIEST)
                        .build())
                .setPollBatchSize(Long.parseLong(envOr("IGGY_POLL_BATCH", "100")))
                .setDeserializer(new TickDeserializer())
                .build();

        DataStream<Tick> ticks = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "iggy-source", TypeInformation.of(Tick.class))
                .map(new NodeCounter()).name("node-counter");

        // PIPELINE=count: source -> node-counter -> discard only, to measure the source on its own.
        if ("count".equals(envOr("PIPELINE", "full"))) {
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

    static String envOr(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }
}
