package com.tailoredbrands.otd.migration.reconciler;

import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pure join of the two sides on {@code orderId}:
 * <ul>
 *   <li>{@code matched}: present on both sides with equal totalAmount (within tolerance) and line count</li>
 *   <li>{@code onlyLegacy}: on EMS/MQ but never seen on Pub/Sub (bridge gap, DLQ, filter)</li>
 *   <li>{@code onlyPubsub}: on Pub/Sub but not on the legacy side (new producers in DUAL_RUN, or lost legacy audit)</li>
 *   <li>{@code payloadMismatch}: both sides, but totalAmount or line count differ (mapping bug)</li>
 * </ul>
 * Duplicates within one side (redeliveries) collapse to one record per orderId — the first one
 * wins — so at-least-once delivery never shows up as a mismatch.
 */
public class ReconciliationEngine {

    private final BigDecimal amountTolerance;

    public ReconciliationEngine(double amountTolerance) {
        this.amountTolerance = BigDecimal.valueOf(amountTolerance);
    }

    public ReconciliationResult reconcile(List<SideRecord> legacy, List<SideRecord> pubsub, Instant from, Instant to,
                                          String phase) {
        Map<String, SideRecord> legacyById = index(legacy);
        Map<String, SideRecord> pubsubById = index(pubsub);

        int matched = 0;
        List<String> onlyLegacy = new ArrayList<>();
        List<String> onlyPubsub = new ArrayList<>();
        List<ReconciliationResult.PayloadMismatch> mismatches = new ArrayList<>();

        for (Map.Entry<String, SideRecord> e : legacyById.entrySet()) {
            SideRecord l = e.getValue();
            SideRecord p = pubsubById.get(e.getKey());
            if (p == null) {
                onlyLegacy.add(e.getKey());
                continue;
            }
            String reason = compare(l, p);
            if (reason == null) {
                matched++;
            } else {
                mismatches.add(new ReconciliationResult.PayloadMismatch(e.getKey(), l.totalAmount(), p.totalAmount(),
                        l.lineCount(), p.lineCount(), reason));
            }
        }
        for (String id : pubsubById.keySet()) {
            if (!legacyById.containsKey(id)) {
                onlyPubsub.add(id);
            }
        }
        onlyLegacy.sort(null);
        onlyPubsub.sort(null);
        mismatches.sort((a, b) -> a.orderId().compareTo(b.orderId()));

        return new ReconciliationResult(Instant.now(), from, to, phase, legacyById.size(), pubsubById.size(), matched,
                List.copyOf(onlyLegacy), List.copyOf(onlyPubsub), List.copyOf(mismatches));
    }

    /** Null when equal; otherwise a short human-readable reason. */
    String compare(SideRecord legacy, SideRecord pubsub) {
        List<String> reasons = new ArrayList<>();
        if (legacy.totalAmount() != null && pubsub.totalAmount() != null) {
            BigDecimal diff = legacy.totalAmount().subtract(pubsub.totalAmount()).abs();
            if (diff.compareTo(amountTolerance) > 0) {
                reasons.add("totalAmount " + legacy.totalAmount().toPlainString() + " != "
                        + pubsub.totalAmount().toPlainString());
            }
        } else if (legacy.totalAmount() != null || pubsub.totalAmount() != null) {
            reasons.add("totalAmount missing on one side");
        }
        if (legacy.lineCount() != null && pubsub.lineCount() != null) {
            if (!Objects.equals(legacy.lineCount(), pubsub.lineCount())) {
                reasons.add("lineCount " + legacy.lineCount() + " != " + pubsub.lineCount());
            }
        } else if (legacy.lineCount() != null || pubsub.lineCount() != null) {
            reasons.add("lineCount missing on one side");
        }
        return reasons.isEmpty() ? null : String.join("; ", reasons);
    }

    private static Map<String, SideRecord> index(List<SideRecord> records) {
        Map<String, SideRecord> byId = new LinkedHashMap<>();
        if (records == null) {
            return byId;
        }
        for (SideRecord r : records) {
            if (r == null || r.orderId() == null || r.orderId().isBlank()) {
                continue;
            }
            byId.putIfAbsent(r.orderId().trim(), r);
        }
        return byId;
    }
}
