package scalable.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iggy.connector.flink.source.IggySource;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.LongAdder;

import static scalable.flink.ConnectorTestJob.envOr;

/**
 * PIPELINE=jdbc: tests the Flink JDBC connector (flink-connector-jdbc-core 4.1.0-2.2) on Flink 2.3 with TimescaleDB.
 *
 * <pre>
 *   IggySource (price events only) → watermarks (bounded out-of-orderness, idleness)
 *     → keyBy(symbol) → 5-min tumbling event-time windows: last price by event time, tick count
 *     → JdbcSink: INSERT ... ON CONFLICT (symbol, window_end) DO UPDATE  (writes = writes + 1)
 *   late events → side output → counter
 * </pre>
 */
final class JdbcSinkPipeline {

    private static final OutputTag<PriceTick> LATE = new OutputTag<>("late", TypeInformation.of(PriceTick.class));

    private static final String UPSERT = """
            INSERT INTO window_close (symbol, window_start, window_end, close, ticks, last_event_time, written_at, writes)
            VALUES (?, ?, ?, ?, ?, ?, now(), 1)
            ON CONFLICT (symbol, window_end) DO UPDATE SET
                window_start = EXCLUDED.window_start,
                close = EXCLUDED.close,
                ticks = EXCLUDED.ticks,
                last_event_time = EXCLUDED.last_event_time,
                written_at = now(),
                writes = window_close.writes + 1
            """;

    private JdbcSinkPipeline() {}

    static void build(StreamExecutionEnvironment env, IggySource<PriceTick> source) {
        long outOfOrdernessS = Long.parseLong(envOr("OUT_OF_ORDERNESS_S", "60"));
        long idlenessS = Long.parseLong(envOr("IDLENESS_S", "10")); // 0 = no idleness

        WatermarkStrategy<PriceTick> wm = WatermarkStrategy
                .<PriceTick>forBoundedOutOfOrderness(Duration.ofSeconds(outOfOrdernessS))
                .withTimestampAssigner((t, ts) -> t.eventTime);
        if (idlenessS > 0) {
            wm = wm.withIdleness(Duration.ofSeconds(idlenessS));
        }

        DataStream<PriceTick> prices = env.fromSource(source, wm, "iggy-source", TypeInformation.of(PriceTick.class));

        SingleOutputStreamOperator<WindowClose> windows = prices
                .keyBy(t -> t.symbol)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
                .sideOutputLateData(LATE)
                .aggregate(new LastPrice(), new ToWindowClose())
                .name("window-5min");

        windows.getSideOutput(LATE)
                .map(new Counter<PriceTick>("late-events")).name("late-counter")
                .sinkTo(new DiscardingSink<>()).name("discard-late");

        windows.map(new Counter<WindowClose>("windows-emitted")).name("window-counter")
                .sinkTo(JdbcSink.<WindowClose>builder()
                        .withQueryStatement(UPSERT, (ps, w) -> {
                            ps.setString(1, w.symbol);
                            ps.setObject(2, utc(w.windowStart));
                            ps.setObject(3, utc(w.windowEnd));
                            ps.setDouble(4, w.close);
                            ps.setLong(5, w.ticks);
                            ps.setObject(6, utc(w.lastEventTime));
                        })
                        .withExecutionOptions(JdbcExecutionOptions.builder()
                                .withBatchSize(Integer.parseInt(envOr("JDBC_BATCH_SIZE", "1000")))
                                .withBatchIntervalMs(1000)
                                .withMaxRetries(3)
                                .build())
                        .buildAtLeastOnce(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(envOr("JDBC_URL", "jdbc:postgresql://timescaledb:5432/trading"))
                                .withDriverName("org.postgresql.Driver")
                                .withUsername(envOr("JDBC_USER", "postgres"))
                                .withPassword(envOr("JDBC_PASSWORD", "postgres"))
                                .build()))
                .name("timescaledb-sink");
    }

    private static OffsetDateTime utc(long epochMs) {
        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMs), ZoneOffset.UTC);
    }

    /** Accumulator: last price by event time (ties broken by Iggy offset, so the result is deterministic). */
    public static class Acc {
        public long eventTime = Long.MIN_VALUE;
        public long offset = Long.MIN_VALUE;
        public double price;
        public long ticks;
    }

    static class LastPrice implements AggregateFunction<PriceTick, Acc, Acc> {
        @Override
        public Acc createAccumulator() {
            return new Acc();
        }

        @Override
        public Acc add(PriceTick t, Acc a) {
            a.ticks++;
            if (t.eventTime > a.eventTime || (t.eventTime == a.eventTime && t.offset > a.offset)) {
                a.eventTime = t.eventTime;
                a.offset = t.offset;
                a.price = t.price;
            }
            return a;
        }

        @Override
        public Acc getResult(Acc a) {
            return a;
        }

        @Override
        public Acc merge(Acc a, Acc b) {
            Acc winner = (b.eventTime > a.eventTime || (b.eventTime == a.eventTime && b.offset > a.offset)) ? b : a;
            Acc m = new Acc();
            m.eventTime = winner.eventTime;
            m.offset = winner.offset;
            m.price = winner.price;
            m.ticks = a.ticks + b.ticks;
            return m;
        }
    }

    static class ToWindowClose extends ProcessWindowFunction<Acc, WindowClose, String, TimeWindow> {
        @Override
        public void process(String symbol, Context ctx, Iterable<Acc> in, Collector<WindowClose> out) {
            Acc a = in.iterator().next();
            WindowClose w = new WindowClose();
            w.symbol = symbol;
            w.windowStart = ctx.window().getStart();
            w.windowEnd = ctx.window().getEnd();
            w.close = a.price;
            w.ticks = a.ticks;
            w.lastEventTime = a.eventTime;
            out.collect(w);
        }
    }

    /** Pass-through that logs a running count per subtask. */
    static class Counter<T> extends RichMapFunction<T, T> {
        private final String name;
        private transient LongAdder count;
        private transient Reporter reporter;

        Counter(String name) {
            this.name = name;
        }

        @Override
        public void open(OpenContext ctx) {
            int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            count = new LongAdder();
            reporter = Reporter.start(() -> String.format("[%s subtask=%d] total=%d", name, subtask, count.sum()));
        }

        @Override
        public T map(T value) {
            count.increment();
            return value;
        }

        @Override
        public void close() {
            if (reporter != null) {
                reporter.stop();
            }
        }
    }
}
