package com.tailoredbrands.otd.migration.reconciler;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Command line of the reconciler:
 *
 * <pre>
 *  --from=2026-10-03T00:00:00Z   --to=2026-10-04T00:00:00Z      window (RFC-3339 or yyyy-MM-dd; default: last 24 h)
 *  --phase=SHADOW                                                 recorded in the report (default: MIGRATION_PHASE env)
 *  --legacyFile=legacy.csv|.jsonl   OR   --legacyQueue=TB.ORDERS.AUDIT   (JMS browse, needs JMS_* env)
 *  --pubsubFile=events.jsonl        OR   --bigQueryDataset=otd [--bigQueryProject=..] [--bigQueryTable=order_events]
 *  --out=report                                                   writes report.json + report.md (stdout otherwise)
 *  --bigQueryDataset=otd                                          also inserts a row into otd.migration_reconciliation
 *  --failOnDiff=true                                              exit code 2 when differences &gt; 0
 *  --amountTolerance=0.01                                         max |legacy - pubsub| total amount considered equal
 * </pre>
 */
public record ReconcilerOptions(
        Instant from,
        Instant to,
        String phase,
        String legacyFile,
        String legacyQueue,
        String pubsubFile,
        String bigQueryProject,
        String bigQueryDataset,
        String bigQueryTable,
        String resultTable,
        String out,
        boolean failOnDiff,
        double amountTolerance) {

    public static final String DEFAULT_LEGACY_QUEUE = "TB.ORDERS.AUDIT";
    public static final String DEFAULT_EVENTS_TABLE = "order_events";
    public static final String DEFAULT_RESULT_TABLE = "migration_reconciliation";

    public boolean usesLegacyFile() {
        return legacyFile != null && !legacyFile.isBlank();
    }

    public boolean usesPubsubFile() {
        return pubsubFile != null && !pubsubFile.isBlank();
    }

    public boolean usesBigQuery() {
        return bigQueryDataset != null && !bigQueryDataset.isBlank();
    }

    public static ReconcilerOptions parse(String[] args) {
        return parse(List.of(args));
    }

    public static ReconcilerOptions parse(List<String> args) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (int i = 0; i < args.size(); i++) {
            String a = args.get(i);
            if (!a.startsWith("--")) {
                continue;
            }
            String body = a.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                kv.put(body.substring(0, eq), body.substring(eq + 1));
            } else if (i + 1 < args.size() && !args.get(i + 1).startsWith("--")) {
                kv.put(body, args.get(++i));
            } else {
                kv.put(body, "true");
            }
        }
        Instant to = kv.containsKey("to") ? parseInstant(kv.get("to"), "to") : Instant.now();
        Instant from = kv.containsKey("from") ? parseInstant(kv.get("from"), "from") : to.minus(Duration.ofHours(24));
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("--from must be before --to (" + from + " >= " + to + ")");
        }
        String phase = kv.getOrDefault("phase", envOr("MIGRATION_PHASE", "UNKNOWN"));
        return new ReconcilerOptions(
                from,
                to,
                phase,
                kv.get("legacyFile"),
                kv.getOrDefault("legacyQueue", DEFAULT_LEGACY_QUEUE),
                kv.get("pubsubFile"),
                kv.getOrDefault("bigQueryProject", envOr("PUBSUB_PROJECT", null)),
                kv.get("bigQueryDataset"),
                kv.getOrDefault("bigQueryTable", DEFAULT_EVENTS_TABLE),
                kv.getOrDefault("resultTable", DEFAULT_RESULT_TABLE),
                kv.get("out"),
                Boolean.parseBoolean(kv.getOrDefault("failOnDiff", "false")),
                Double.parseDouble(kv.getOrDefault("amountTolerance", "0.005")));
    }

    static Instant parseInstant(String value, String what) {
        String v = value.trim();
        try {
            return Instant.parse(v);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDate.parse(v).atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("--" + what + " must be RFC-3339 or yyyy-MM-dd: " + value, e);
        }
    }

    private static String envOr(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }
}
