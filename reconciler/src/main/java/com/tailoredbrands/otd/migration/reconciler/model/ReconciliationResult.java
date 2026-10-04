package com.tailoredbrands.otd.migration.reconciler.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Outcome of one reconciliation run; serialised to JSON / markdown and to BigQuery. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ReconciliationResult(
        Instant runTime,
        Instant from,
        Instant to,
        String phase,
        int legacyCount,
        int pubsubCount,
        int matched,
        List<String> onlyLegacy,
        List<String> onlyPubsub,
        List<PayloadMismatch> payloadMismatch) {

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PayloadMismatch(
            String orderId,
            BigDecimal legacyTotalAmount,
            BigDecimal pubsubTotalAmount,
            Integer legacyLineCount,
            Integer pubsubLineCount,
            String reason) {
    }

    public int differences() {
        return onlyLegacy.size() + onlyPubsub.size() + payloadMismatch.size();
    }

    /** True when both sides agree completely: the exit criterion of phases 1 and 2. */
    public boolean clean() {
        return differences() == 0;
    }
}
