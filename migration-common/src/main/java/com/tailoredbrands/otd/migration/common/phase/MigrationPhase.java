package com.tailoredbrands.otd.migration.common.phase;

import java.util.Locale;

/**
 * Migration phases of ARCHITECTURE.md section 7. The two bridges derive their running state
 * from the phase:
 *
 * <pre>
 *  phase            jms-to-pubsub-bridge   pubsub-to-jms-bridge
 *  0 LEGACY_ONLY    paused                 paused
 *  1 SHADOW         running                paused
 *  2 DUAL_RUN       running                running
 *  3 PUBSUB_PRIMARY running (drain only)   running
 *  4 CUTOVER        paused                 paused
 * </pre>
 */
public enum MigrationPhase {

    LEGACY_ONLY(0, false, false),
    SHADOW(1, true, false),
    DUAL_RUN(2, true, true),
    PUBSUB_PRIMARY(3, true, true),
    CUTOVER(4, false, false);

    private final int number;
    private final boolean legacyToPubSub;
    private final boolean pubSubToLegacy;

    MigrationPhase(int number, boolean legacyToPubSub, boolean pubSubToLegacy) {
        this.number = number;
        this.legacyToPubSub = legacyToPubSub;
        this.pubSubToLegacy = pubSubToLegacy;
    }

    /** 0..4 as used in the runbook. */
    public int number() {
        return number;
    }

    /** True when the EMS → Pub/Sub bridge must consume. */
    public boolean bridgesLegacyToPubSub() {
        return legacyToPubSub;
    }

    /** True when the Pub/Sub → MQ bridge must consume. */
    public boolean bridgesPubSubToLegacy() {
        return pubSubToLegacy;
    }

    /** Lenient parse: name, "phase-2", "2", lower case, surrounding whitespace / quotes. */
    public static MigrationPhase parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Migration phase is null");
        }
        String v = value.trim().replace("\"", "").replace("'", "").toUpperCase(Locale.ROOT).replace('-', '_');
        if (v.startsWith("PHASE_")) {
            v = v.substring("PHASE_".length());
        }
        if (v.length() == 1 && Character.isDigit(v.charAt(0))) {
            int n = v.charAt(0) - '0';
            for (MigrationPhase p : values()) {
                if (p.number == n) {
                    return p;
                }
            }
        }
        return MigrationPhase.valueOf(v);
    }
}
