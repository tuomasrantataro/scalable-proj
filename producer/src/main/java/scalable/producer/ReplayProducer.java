package scalable.producer;

import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.identifier.ConsumerId;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Minimal replay producer.
 *
 * <ul>
 *   <li>{@code setup}: creates stream, topic (N partitions) and the Flink consumer group, idempotently.</li>
 *   <li>{@code replay}: sends every CSV line (header skipped) to the topic, partitioned by symbol ID.</li>
 * </ul>
 *
 * All settings come from environment variables, see {@link #env}.
 */
public final class ReplayProducer {

    private static final String HOST = env("IGGY_HOST", "iggy");
    private static final int PORT = Integer.parseInt(env("IGGY_PORT", "8090"));
    private static final String USER = env("IGGY_USERNAME", "iggy");
    private static final String PASS = env("IGGY_PASSWORD", "iggy");
    private static final String STREAM = env("IGGY_STREAM", "trading");
    private static final String TOPIC = env("IGGY_TOPIC", "ticks");
    private static final String GROUP = env("IGGY_CONSUMER_GROUP", "flink");
    private static final int PARTITIONS = Integer.parseInt(env("IGGY_PARTITIONS", "4"));

    private static final String CSV_FILE = env("CSV_FILE", "/data/pala.csv");
    /** How many times to replay the file (to generate more volume). */
    private static final int REPEAT = Integer.parseInt(env("REPEAT", "1"));
    /** Target messages per second, 0 = as fast as possible. */
    private static final int RATE = Integer.parseInt(env("RATE", "0"));
    private static final int BATCH = Integer.parseInt(env("BATCH_SIZE", "4000"));

    private ReplayProducer() {}

    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "replay";
        try (IggyTcpClient client = connect()) {
            switch (mode) {
                case "setup" -> setup(client);
                case "replay" -> replay(client);
                default -> throw new IllegalArgumentException("unknown mode: " + mode + " (setup|replay)");
            }
        }
    }

    private static IggyTcpClient connect() throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                IggyTcpClient c = IggyTcpClient.builder()
                        .host(HOST).port(PORT).credentials(USER, PASS).buildAndLogin();
                log("connected to %s:%d", HOST, PORT);
                return c;
            } catch (RuntimeException e) {
                if (attempt >= 30) {
                    throw e;
                }
                log("iggy not ready (%s), retrying...", e.getMessage());
                Thread.sleep(1000);
            }
        }
    }

    private static void setup(IggyTcpClient client) {
        StreamId stream = StreamId.of(STREAM);
        TopicId topic = TopicId.of(TOPIC);

        if (lookup(() -> client.streams().getStream(stream)).isEmpty()) {
            client.streams().createStream(STREAM);
            log("created stream %s", STREAM);
        }
        if (lookup(() -> client.topics().getTopic(stream, topic)).isEmpty()) {
            client.topics().createTopic(stream, TOPIC, PARTITIONS);
            log("created topic %s with %d partitions", TOPIC, PARTITIONS);
        }
        if (lookup(() -> client.consumerGroups().getConsumerGroup(stream, topic, ConsumerId.of(GROUP))).isEmpty()) {
            client.consumerGroups().createConsumerGroup(stream, topic, GROUP);
            log("created consumer group %s", GROUP);
        }
        log("setup done: %s", client.topics().getTopic(stream, topic).orElseThrow());
    }

    /** Some SDK lookups return empty, others throw on "not found"; treat both as absent. */
    private static <T> Optional<T> lookup(java.util.function.Supplier<Optional<T>> s) {
        try {
            return s.get();
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static void replay(IggyTcpClient client) throws Exception {
        StreamId stream = StreamId.of(STREAM);
        TopicId topic = TopicId.of(TOPIC);
        int partitions = client.topics().getTopic(stream, topic).orElseThrow().partitionsCount().intValue();
        long start = System.nanoTime();
        long sent = 0;
        Map<String, Long> perSymbol = new LinkedHashMap<>();

        for (int round = 0; round < REPEAT; round++) {
            try (BufferedReader in = Files.newBufferedReader(Path.of(CSV_FILE), StandardCharsets.UTF_8)) {
                List<String> batch = new ArrayList<>(BATCH);
                String line;
                while ((line = in.readLine()) != null) {
                    // DEBS files: '#' comment lines (licence, column descriptions) and the "ID,SecType,..." header
                    if (line.isBlank() || line.startsWith("#") || line.startsWith("ID,")) {
                        continue;
                    }
                    batch.add(line);
                    if (batch.size() >= BATCH) {
                        sent += flush(client, stream, topic, partitions, batch, perSymbol);
                        pace(start, sent);
                    }
                }
                sent += flush(client, stream, topic, partitions, batch, perSymbol);
            }
        }

        double secs = (System.nanoTime() - start) / 1e9;
        log("replay done: sent %d messages (%d distinct symbols) in %.2f s", sent, perSymbol.size(), secs);
    }

    /**
     * Sends a batch with one send call per partition. The partition is chosen here from the symbol hash
     * (instead of Iggy's per-call messagesKey), so a batch can mix symbols while each symbol still always
     * lands in the same partition.
     */
    private static int flush(IggyTcpClient client, StreamId stream, TopicId topic, int partitions,
                             List<String> lines, Map<String, Long> perSymbol) {
        Map<Integer, List<Message>> byPartition = new LinkedHashMap<>();
        for (String l : lines) {
            String symbol = l.substring(0, l.indexOf(','));
            int p = Math.floorMod(symbol.hashCode(), partitions);
            byPartition.computeIfAbsent(p, k -> new ArrayList<>()).add(Message.of(l));
            perSymbol.merge(symbol, 1L, Long::sum);
        }
        for (Map.Entry<Integer, List<Message>> e : byPartition.entrySet()) {
            client.messages().sendMessages(stream, topic, Partitioning.partitionId((long) e.getKey()), e.getValue());
        }
        int n = lines.size();
        lines.clear();
        return n;
    }

    private static void pace(long startNanos, long sent) throws InterruptedException {
        if (RATE <= 0) {
            return;
        }
        long targetNanos = startNanos + (long) (sent * 1e9 / RATE);
        long sleepMs = (targetNanos - System.nanoTime()) / 1_000_000;
        if (sleepMs > 0) {
            Thread.sleep(sleepMs);
        }
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    private static void log(String fmt, Object... args) {
        System.out.printf("[replay-producer] " + fmt + "%n", args);
    }
}
