package com.tailoredbrands.otd.migration.reconciler;

import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import com.tailoredbrands.otd.migration.reconciler.source.FileRecordSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FileRecordSourceTest {

    static final Instant FROM = Instant.parse("2026-10-03T00:00:00Z");
    static final Instant TO = Instant.parse("2026-10-04T00:00:00Z");

    static Path resource(String name) throws Exception {
        return Paths.get(FileRecordSourceTest.class.getClassLoader().getResource(name).toURI());
    }

    @Test
    void readsCsvAndAppliesWindow() throws Exception {
        List<SideRecord> records = new FileRecordSource(resource("legacy-orders.csv"), "legacy").read(FROM, TO);
        // 7 rows, one outside the window; the duplicate stays here and is collapsed by the engine
        assertThat(records).hasSize(6);
        SideRecord first = records.get(0);
        assertThat(first.orderId()).isEqualTo("ORD-2026-000101");
        assertThat(first.totalAmount()).isEqualByComparingTo("649.99");
        assertThat(first.lineCount()).isEqualTo(2);
        assertThat(first.timestamp()).isEqualTo(Instant.parse("2026-10-03T10:00:00Z"));
        assertThat(first.messageId()).isEqualTo("ID:EMS.1");
        assertThat(records).noneMatch(r -> r.orderId().equals("ORD-2026-000199"));
    }

    @Test
    void readsJsonLinesInCanonicalAndFlattenedShapesAndSkipsGarbage() throws Exception {
        List<SideRecord> records = new FileRecordSource(resource("pubsub-events.jsonl"), "pubsub").read(FROM, TO);
        assertThat(records).hasSize(5);
        assertThat(records.get(0).orderId()).isEqualTo("ORD-2026-000101");
        assertThat(records.get(0).lineCount()).isEqualTo(2);
        assertThat(records.get(0).messageId()).isEqualTo("ID:EMS.1");
        assertThat(records.get(0).timestamp()).isEqualTo(Instant.parse("2026-10-03T10:00:01Z"));
        SideRecord bq = records.get(2);
        assertThat(bq.orderId()).isEqualTo("ORD-2026-000103");
        assertThat(bq.totalAmount()).isEqualByComparingTo("1250.50");
        assertThat(bq.lineCount()).isEqualTo(2);
        assertThat(bq.timestamp()).isEqualTo(Instant.parse("2026-10-03T10:10:01Z"));
        assertThat(records.get(4).orderId()).isEqualTo("ORD-2026-000106");
        assertThat(records.get(4).messageId()).isEqualTo("e6");
    }

    @Test
    void readsLegacyJsonLinesWithEmbeddedXml() throws Exception {
        List<SideRecord> records = new FileRecordSource(resource("legacy-orders.jsonl"), "legacy")
                .read(Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z"));
        assertThat(records).hasSize(2);
        assertThat(records.get(0).orderId()).isEqualTo("ORD-X-1");
        assertThat(records.get(0).totalAmount()).isEqualTo(new BigDecimal("10.00"));
        assertThat(records.get(0).lineCount()).isEqualTo(1);
        assertThat(records.get(0).timestamp()).isEqualTo(Instant.parse("2026-10-03T12:00:00Z"));
        assertThat(records.get(0).messageId()).isEqualTo("ID:EMS.X1");
        assertThat(records.get(1).timestamp()).isEqualTo(Instant.ofEpochMilli(1_790_000_000_000L));
        assertThat(records.get(1).totalAmount()).isEqualByComparingTo("20");
    }
}
