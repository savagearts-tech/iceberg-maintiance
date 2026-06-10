package io.github.iceberg.cleanup.scan;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TimeWindowPrefixGeneratorTest {

    @Test
    void testGenerateHourlyPrefixes() {
        Schema schema = new Schema(Types.NestedField.required(1, "ts", Types.TimestampType.withZone()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).hour("ts").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.spec()).thenReturn(spec);

        TimeWindowPrefixGenerator generator = new TimeWindowPrefixGenerator(table, "s3a://bucket/data/");
        List<String> prefixes = generator.generate(3, 24); // 3 days ago, 24-hour window

        System.out.println("Generated hourly prefixes: " + prefixes);
        assertEquals(25, prefixes.size(), "Should generate 25 prefixes for a 24-hour window (inclusive)");
        assertTrue(prefixes.get(0).startsWith("s3a://bucket/data/ts_hour="));
    }

    @Test
    void testGenerateDailyPrefixes() {
        Schema schema = new Schema(Types.NestedField.required(1, "event_date", Types.DateType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).day("event_date").build();
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.spec()).thenReturn(spec);

        TimeWindowPrefixGenerator generator = new TimeWindowPrefixGenerator(table, "s3a://bucket/data");
        List<String> prefixes = generator.generate(1, 48); // 1 day ago, 48-hour window

        System.out.println("Generated daily prefixes: " + prefixes);
        assertEquals(3, prefixes.size(), "Should generate 3 prefixes for 48-hour window by day");
        assertTrue(prefixes.get(0).startsWith("s3a://bucket/data/event_date_day="));
    }
}
