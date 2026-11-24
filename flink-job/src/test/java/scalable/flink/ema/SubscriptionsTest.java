package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SubscriptionsTest {

    static Subscriptions parse(String... lines) {
        return Subscriptions.parse(List.of(lines), "test");
    }

    @Test
    void perTraderLists() {
        Subscriptions s = parse(
                "# comment line",
                "",
                "bob:   ASML.NL, TTE.FR   # trailing comment",
                "alice: TTE.FR PX1.FR,623100.ETR",
                "bob:   PX1.FR");                       // same trader again: combined
        assertEquals(List.of("alice", "bob"), s.tradersOf("TTE.FR"));
        assertEquals(List.of("bob"), s.tradersOf("ASML.NL"));
        assertEquals(List.of("alice", "bob"), s.tradersOf("PX1.FR"));
        assertEquals(List.of("alice"), s.tradersOf("623100.ETR"));
        assertTrue(s.tradersOf("OTHER.NL").isEmpty());
        assertEquals(6, s.subscriptionCount());
    }

    @Test
    void wildcardSubscribesToEverySymbolOnce() {
        Subscriptions s = parse("all: *", "bob: ASML.NL", "both: * ASML.NL");
        assertEquals(List.of("all", "bob", "both"), s.tradersOf("ASML.NL"));
        assertEquals(List.of("all", "both"), s.tradersOf("ANY.FR"));
    }

    @Test
    void duplicatesCountOnce() {
        Subscriptions s = parse("bob: ASML.NL ASML.NL", "bob: ASML.NL");
        assertEquals(List.of("bob"), s.tradersOf("ASML.NL"));
        assertEquals(1, s.subscriptionCount());
    }

    @Test
    void emptyFileMeansNoDeliveries() {
        Subscriptions s = parse("# nobody yet");
        assertEquals(0, s.subscriptionCount());
        assertTrue(s.tradersOf("ASML.NL").isEmpty());
    }

    @Test
    void malformedLinesAreErrors() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> parse("bob: A.FR", "ASML.NL"));
        assertTrue(e.getMessage().startsWith("test:2:"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> parse("bad name: A.FR"));
        assertThrows(IllegalArgumentException.class, () -> parse("bob: A.FR; B.FR"));
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        // the filter function, with its Subscriptions, is shipped from the JobManager to the TaskManagers
        Subscriptions s = parse("bob: ASML.NL", "all: *");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(s);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            Subscriptions copy = (Subscriptions) in.readObject();
            assertEquals(List.of("all", "bob"), copy.tradersOf("ASML.NL"));
        }
    }
}
