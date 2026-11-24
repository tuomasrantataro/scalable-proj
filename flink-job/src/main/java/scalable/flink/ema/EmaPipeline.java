package scalable.flink.ema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scalable.flink.Reporter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.LongAdder;

import static scalable.flink.TradingJob.envOr;
import static scalable.flink.TradingJob.bootstrapServers;
import static scalable.flink.TradingJob.source;
import static scalable.flink.TradingJob.withIdlenessAndAlignment;

/**
 * Query 1 (EMA38/EMA100 per 5-min window) and Query 2 (BUY/SELL crossovers), with a data
 * quality report. Architecture §3.2, §3.5.
 *
 * <pre>
 *   KafkaSource → MarketEvent (price rows only) → watermarks (bounded out-of-orderness D, idleness)
 *     → QualityGate ──(dropped / suspicious)──────────────────────────────┐
 *     → keyBy(symbol) → 5-min tumbling event-time window: close, ticks     │
 *         └─(late events)─────────────────────────────────────────────────┤
 *     → keyBy(symbol) → EmaFunction (keyed EmaState) ──(price jumps)───────┤
 *     ├→ JdbcSink: ema_window (upsert on symbol, window_end; own task)       → JdbcSink: data_issue
 *     └→ advisories only (BUY/SELL) → SubscriptionFilter: one copy per subscribed trader
 *          → KafkaSink: topic `advisories` (key symbol, JSON value; one record per trader and advisory)
 * </pre>
 *
 * Subscriptions (FR-4) come from SUBSCRIPTIONS_FILE (default /config/subscriptions.txt), read once at job
 * start; see {@link Subscriptions}. EMAs and the ema_window table still cover every symbol (FR-3).
 *
 * Advisory delivery is at-least-once: each record is sent at once (linger.ms=0), and the sink flushes at every
 * checkpoint, so after a failure some advisories can be sent again. (trader, symbol, window_end) identifies a message, so consumers can drop repeats.
 *
 * Settings (env): OUT_OF_ORDERNESS_S (1, may be fractional), IDLENESS_S (10, 0 = off), WATERMARK_ALIGNMENT_S (300, 0 = off),
 * SKEW_REPORT_S (3600),
 * EMA_SINK (jdbc; discard = do not write ema_window, to measure what the database costs),
 * PRICE_JUMP_REPORT (0.10), JDBC_URL / JDBC_USER / JDBC_PASSWORD, JDBC_BATCH_SIZE (1000),
 * KAFKA_BOOTSTRAP, KAFKA_ADVISORIES_TOPIC (advisories), SUBSCRIPTIONS_FILE.
 */
public final class EmaPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(EmaPipeline.class);

    static final OutputTag<MarketEvent> LATE = new OutputTag<>("late", TypeInformation.of(MarketEvent.class));

    private static final String UPSERT_EMA = """
            INSERT INTO ema_window (symbol, sec_type, window_start, window_end, close, close_time, ticks,
                                    ema38, ema100, prev_ema38, prev_ema100, advisory, initial, fired_at, emitted_at,
                                    writes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT (symbol, window_end) DO UPDATE SET
                sec_type = EXCLUDED.sec_type, window_start = EXCLUDED.window_start, close = EXCLUDED.close,
                close_time = EXCLUDED.close_time, ticks = EXCLUDED.ticks, ema38 = EXCLUDED.ema38,
                ema100 = EXCLUDED.ema100, prev_ema38 = EXCLUDED.prev_ema38, prev_ema100 = EXCLUDED.prev_ema100,
                advisory = EXCLUDED.advisory, initial = EXCLUDED.initial, fired_at = EXCLUDED.fired_at,
                emitted_at = EXCLUDED.emitted_at,
                writes = ema_window.writes + 1
            """;

    private static final String INSERT_ISSUE = """
            INSERT INTO data_issue (reason, symbol, event_time, price, kafka_partition, kafka_offset, detail)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private EmaPipeline() {}

    public static void build(StreamExecutionEnvironment env) throws IOException {
        Path subscriptionsFile = Path.of(envOr("SUBSCRIPTIONS_FILE", "/config/subscriptions.txt"));
        Subscriptions subscriptions = Subscriptions.load(subscriptionsFile);
        LOG.info("subscriptions from {}: {}", subscriptionsFile, subscriptions);
        if (subscriptions.subscriptionCount() == 0) {
            LOG.warn("no subscriptions in {}: no advisories will be delivered", subscriptionsFile);
        }
        Duration outOfOrderness = seconds(envOr("OUT_OF_ORDERNESS_S", "1"));
        long skewS = Long.parseLong(envOr("SKEW_REPORT_S", "3600"));
        double jump = Double.parseDouble(envOr("PRICE_JUMP_REPORT", "0.10"));

        // Every event with a valid time advances the watermark, rejected ones included: on the weekend only
        // zero prices arrive, and without them Friday's last windows would never be evaluated. Events with a
        // malformed or implausible time get no timestamp, so they cannot move the watermark.
        WatermarkStrategy<MarketEvent> wm = WatermarkStrategy
                .<MarketEvent>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner((e, ts) -> e.timeValid ? e.eventTime : Long.MIN_VALUE);
        wm = withIdlenessAndAlignment(wm); // architecture §3.4

        SingleOutputStreamOperator<MarketEvent> prices = env
                .fromSource(source(new MarketEventDeserializer()), wm, "kafka-source", TypeInformation.of(MarketEvent.class))
                .process(new QualityGate(skewS * 1000)).name("quality-gate");

        SingleOutputStreamOperator<PriceWindow> windows = prices
                .keyBy(e -> e.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .sideOutputLateData(LATE)
                .aggregate(new ClosePrice.Aggregate(), new ClosePrice.ToPriceWindow())
                .name("window-5min");

        SingleOutputStreamOperator<EmaWindow> emas = windows
                .keyBy(w -> w.symbol)
                .process(new EmaFunction(jump)).name("ema");

        DataStream<DataIssue> issues = prices.getSideOutput(QualityGate.ISSUES)
                .union(windows.getSideOutput(LATE).map(new LateToIssue()).name("late-events"),
                        emas.getSideOutput(EmaFunction.ISSUES));

        // Own task (not chained to the EMA function): the JDBC sink upserts synchronously in batches, and chained it
        // blocked the EMA thread, so closed windows queued behind each batch. Unchained, window close → advisory
        // received stayed under ~100 ms at every input rate; chained, p95 reached 628 ms (MEASUREMENTS.md §6).
        boolean writes = writesEma(envOr("EMA_SINK", "jdbc"));
        if (!writes) {
            LOG.warn("EMA_SINK=discard: ema_window is not written");
        }
        Sink<EmaWindow> emaSink = writes ? jdbcSink(UPSERT_EMA, (ps, r) -> {
            ps.setString(1, r.symbol);
            ps.setString(2, r.secType);
            ps.setObject(3, ts(r.windowStart));
            ps.setObject(4, ts(r.windowEnd));
            ps.setDouble(5, r.close);
            ps.setObject(6, ts(r.closeTime));
            ps.setLong(7, r.ticks);
            ps.setDouble(8, r.ema38);
            ps.setDouble(9, r.ema100);
            ps.setDouble(10, r.prevEma38);
            ps.setDouble(11, r.prevEma100);
            if (r.advisory == null) {
                ps.setNull(12, Types.VARCHAR);
            } else {
                ps.setString(12, r.advisory);
            }
            ps.setBoolean(13, r.initial);
            ps.setObject(14, ts(r.firedAt));
            ps.setObject(15, ts(r.emittedAt));
        }) : new DiscardingSink<>();
        emas.sinkTo(emaSink).name(writes ? "timescaledb-ema" : "discard-ema").disableChaining();

        // Query 2 output (FR-15, FR-16): each advisory is sent as soon as its window is evaluated (linger.ms=0).
        // Key = symbol, so all advisories of a symbol go to one partition, in window order.
        // At-least-once: exactly-once would use Kafka transactions, which publish only at checkpoints.
        emas.filter(r -> r.advisory != null).name("advisories-only")
                .flatMap(new SubscriptionFilter(subscriptions)).name("subscriptions")
                .sinkTo(KafkaSink.<TraderAdvisory>builder()
                        .setBootstrapServers(bootstrapServers())
                        .setRecordSerializer(KafkaRecordSerializationSchema.<TraderAdvisory>builder()
                                .setTopic(envOr("KAFKA_ADVISORIES_TOPIC", "advisories"))
                                .setKeySerializationSchema(ta -> ta.advisory.symbol.getBytes(StandardCharsets.UTF_8))
                                .setValueSerializationSchema(new AdvisorySerializer())
                                .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .setProperty("linger.ms", "0")
                        .build())
                .name("kafka-advisories");

        issues.sinkTo(jdbcSink(INSERT_ISSUE, (ps, i) -> {
            ps.setString(1, i.reason);
            ps.setString(2, i.symbol);
            ps.setObject(3, ts(i.eventTime));
            ps.setDouble(4, i.price);
            ps.setInt(5, i.partition);
            ps.setLong(6, i.offset);
            ps.setString(7, i.detail);
        })).name("timescaledb-issues");
    }

    private static <T> JdbcSink<T> jdbcSink(String sql, JdbcStatementBuilder<T> binder) {
        return JdbcSink.<T>builder()
                .withQueryStatement(sql, binder)
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withBatchSize(Integer.parseInt(envOr("JDBC_BATCH_SIZE", "1000")))
                        .withBatchIntervalMs(1000)
                        .withMaxRetries(3)
                        .build())
                .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(envOr("JDBC_URL", "jdbc:postgresql://timescaledb:5432/trading?reWriteBatchedInserts=true"))
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(envOr("JDBC_USER", "postgres"))
                        .withPassword(envOr("JDBC_PASSWORD", "postgres"))
                        .build());
    }

    /** Seconds, possibly fractional ("0.5"), rounded to milliseconds. */
    static Duration seconds(String s) {
        return Duration.ofMillis(Math.round(Double.parseDouble(s) * 1000));
    }

    /** Whether the EMA results go to TimescaleDB. Anything but the two known values is an error. */
    static boolean writesEma(String sink) {
        return switch (sink) {
            case "jdbc" -> true;
            case "discard" -> false;
            default -> throw new IllegalArgumentException("EMA_SINK=" + sink + ": expected jdbc or discard");
        };
    }

    /** Epoch ms of local exchange time (read as UTC) → timestamptz with the same wall-clock value in UTC. */
    static OffsetDateTime ts(long epochMs) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
    }

    /** Late events (window already evaluated) → data issue, with a per-subtask count. */
    static class LateToIssue extends RichMapFunction<MarketEvent, DataIssue> {
        private transient LongAdder late;
        private transient Reporter reporter;

        @Override
        public void open(OpenContext ctx) {
            late = new LongAdder();
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            reporter = Reporter.start(() -> String.format("[late subtask=%d] late=%d", subtask, late.sum()));
        }

        @Override
        public DataIssue map(MarketEvent e) {
            late.increment();
            long windowEnd = e.eventTime - Math.floorMod(e.eventTime, 300_000L) + 300_000L;
            return DataIssue.of(IssueType.LATE, e, "arrived after its window ending "
                    + ts(windowEnd).toLocalDateTime() + " was evaluated");
        }

        @Override
        public void close() {
            if (reporter != null) {
                reporter.stop();
            }
        }
    }
}
