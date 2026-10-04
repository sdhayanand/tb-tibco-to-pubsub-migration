package com.tailoredbrands.otd.migration.reconciler;

import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReconciliationEngineTest {

    private static final Instant FROM = Instant.parse("2026-10-03T00:00:00Z");
    private static final Instant TO = Instant.parse("2026-10-04T00:00:00Z");

    private static SideRecord rec(String id, String total, Integer lines) {
        return new SideRecord(id, total == null ? null : new BigDecimal(total), lines, FROM.plusSeconds(60), "m-" + id);
    }

    @Test
    void joinsOnOrderIdAndClassifies() {
        ReconciliationEngine engine = new ReconciliationEngine(0.005);
        List<SideRecord> legacy = List.of(rec("A", "10.00", 1), rec("B", "20.00", 2), rec("C", "30.00", 3),
                rec("D", "40.00", 1), rec("D", "40.00", 1));
        List<SideRecord> pubsub = List.of(rec("A", "10.00", 1), rec("B", "20.50", 2), rec("C", "30.00", 4),
                rec("E", "50.00", 1));

        ReconciliationResult r = engine.reconcile(legacy, pubsub, FROM, TO, "SHADOW");

        assertThat(r.legacyCount()).isEqualTo(4);
        assertThat(r.pubsubCount()).isEqualTo(4);
        assertThat(r.matched()).isEqualTo(1);
        assertThat(r.onlyLegacy()).containsExactly("D");
        assertThat(r.onlyPubsub()).containsExactly("E");
        assertThat(r.payloadMismatch()).hasSize(2);
        assertThat(r.payloadMismatch().get(0).orderId()).isEqualTo("B");
        assertThat(r.payloadMismatch().get(0).reason()).contains("totalAmount 20.00 != 20.50");
        assertThat(r.payloadMismatch().get(1).orderId()).isEqualTo("C");
        assertThat(r.payloadMismatch().get(1).reason()).contains("lineCount 3 != 4");
        assertThat(r.differences()).isEqualTo(4);
        assertThat(r.clean()).isFalse();
        assertThat(r.phase()).isEqualTo("SHADOW");
    }

    @Test
    void cleanRunWhenBothSidesAgree() {
        ReconciliationEngine engine = new ReconciliationEngine(0.005);
        List<SideRecord> both = List.of(rec("A", "10.00", 1), rec("B", "20.00", 2));
        ReconciliationResult r = engine.reconcile(both, both, FROM, TO, "DUAL_RUN");
        assertThat(r.matched()).isEqualTo(2);
        assertThat(r.clean()).isTrue();
    }

    @Test
    void amountToleranceAndMissingFields() {
        ReconciliationEngine engine = new ReconciliationEngine(0.01);
        assertThat(engine.compare(rec("A", "10.00", 1), rec("A", "10.009", 1))).isNull();
        assertThat(engine.compare(rec("A", "10.00", 1), rec("A", "10.02", 1))).contains("totalAmount");
        assertThat(engine.compare(rec("A", null, 1), rec("A", null, 1))).isNull();
        assertThat(engine.compare(rec("A", "1", 1), rec("A", null, 1))).contains("missing");
        assertThat(engine.compare(rec("A", "1", null), rec("A", "1", null))).isNull();
        assertThat(engine.compare(rec("A", "1", 2), rec("A", "1", null))).contains("lineCount missing");
    }

    @Test
    void ignoresRecordsWithoutOrderId() {
        ReconciliationEngine engine = new ReconciliationEngine(0.005);
        ReconciliationResult r = engine.reconcile(List.of(rec(null, "1", 1), rec(" ", "1", 1)), List.of(), FROM, TO, "x");
        assertThat(r.legacyCount()).isZero();
        assertThat(r.clean()).isTrue();
    }
}
