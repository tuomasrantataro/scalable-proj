package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EmaPipelineTest {
    @Test
    void secondsMayBeFractional() {
        assertEquals(Duration.ofMillis(500), EmaPipeline.seconds("0.5"));
        assertEquals(Duration.ofSeconds(60), EmaPipeline.seconds("60"));
        assertEquals(Duration.ZERO, EmaPipeline.seconds("0"));
    }

    @Test
    void emaSinkIsJdbcUnlessDiscarded() {
        assertTrue(EmaPipeline.writesEma("jdbc"));
        assertFalse(EmaPipeline.writesEma("discard"));
        assertThrows(IllegalArgumentException.class, () -> EmaPipeline.writesEma("Discard"));
    }
}
