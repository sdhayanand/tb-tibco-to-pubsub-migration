package com.tailoredbrands.otd.migration.reconciler;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconcilerOptionsTest {

    @Test
    void parsesKeyValueAndSpaceSeparatedForms() {
        ReconcilerOptions o = ReconcilerOptions.parse(new String[] {
                "--from=2026-10-03T00:00:00Z", "--to", "2026-10-04", "--legacyQueue", "TB.ORDERS.AUDIT",
                "--bigQueryDataset=otd", "--bigQueryProject=my-proj", "--failOnDiff", "--amountTolerance=0.02"});
        assertThat(o.from()).isEqualTo(Instant.parse("2026-10-03T00:00:00Z"));
        assertThat(o.to()).isEqualTo(Instant.parse("2026-10-04T00:00:00Z"));
        assertThat(o.legacyQueue()).isEqualTo("TB.ORDERS.AUDIT");
        assertThat(o.usesLegacyFile()).isFalse();
        assertThat(o.usesBigQuery()).isTrue();
        assertThat(o.bigQueryProject()).isEqualTo("my-proj");
        assertThat(o.bigQueryTable()).isEqualTo("order_events");
        assertThat(o.resultTable()).isEqualTo("migration_reconciliation");
        assertThat(o.failOnDiff()).isTrue();
        assertThat(o.amountTolerance()).isEqualTo(0.02);
    }

    @Test
    void defaultsToLast24Hours() {
        ReconcilerOptions o = ReconcilerOptions.parse(new String[] {});
        assertThat(Duration.between(o.from(), o.to())).isEqualTo(Duration.ofHours(24));
        assertThat(o.failOnDiff()).isFalse();
    }

    @Test
    void rejectsInvertedWindowAndBadDates() {
        assertThatThrownBy(() -> ReconcilerOptions.parse(new String[] {"--from=2026-10-04", "--to=2026-10-03"}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ReconcilerOptions.parse(new String[] {"--from=yesterday"}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("--from");
    }
}
