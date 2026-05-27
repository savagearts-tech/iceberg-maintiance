package io.github.iceberg.common;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import static org.junit.jupiter.api.Assertions.*;

class RetentionConfigTest {

    @Test
    void defaults() {
        RetentionConfig c = RetentionConfig.defaults();
        assertEquals(Duration.ofDays(90), c.expireOlderThan());
        assertEquals(5, c.retainLast());
        assertEquals(3, c.coolingPeriodDays());
    }

    @Test
    void customValues() {
        var c = new RetentionConfig(Duration.ofDays(30), 10, 7);
        assertEquals(30, c.expireOlderThan().toDays());
        assertEquals(10, c.retainLast());
        assertEquals(7, c.coolingPeriodDays());
    }

    @Test
    void retainLastMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetentionConfig(Duration.ofDays(30), 0, 3));
    }

    @Test
    void coolingPeriodMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetentionConfig(Duration.ofDays(30), 5, 0));
    }
}
