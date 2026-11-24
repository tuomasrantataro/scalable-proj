package scalable.flink.ema;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EmaPipelineTest {
    @Test
    void secondsMayBeFractional() {
        assertEquals(Duration.ofMillis(500), EmaPipeline.seconds("0.5"));
        assertEquals(Duration.ofSeconds(60), EmaPipeline.seconds("60"));
        assertEquals(Duration.ZERO, EmaPipeline.seconds("0"));
    }
}
