package com.tailoredbrands.otd.migration.reconciler.report;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;

import java.util.List;

/** Renders a {@link ReconciliationResult} as JSON (machine) and Markdown (humans / runbook evidence). */
public class ReportWriter {

    private static final int MAX_LISTED = 50;

    private final ObjectMapper json = JsonMappers.canonical();

    public String toJson(ReconciliationResult result) {
        try {
            return json.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialise reconciliation result", e);
        }
    }

    public String toMarkdown(ReconciliationResult r, String legacySource, String pubsubSource) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Migration reconciliation — ").append(r.clean() ? "CLEAN ✅" : "DIFFERENCES ❌").append("\n\n");
        sb.append("| | |\n|---|---|\n");
        sb.append("| Run time | ").append(r.runTime()).append(" |\n");
        sb.append("| Window | ").append(r.from()).append(" → ").append(r.to()).append(" |\n");
        sb.append("| Phase | ").append(r.phase()).append(" |\n");
        sb.append("| Legacy source | ").append(legacySource).append(" |\n");
        sb.append("| Pub/Sub source | ").append(pubsubSource).append(" |\n");
        sb.append("| Legacy orders | ").append(r.legacyCount()).append(" |\n");
        sb.append("| Pub/Sub orders | ").append(r.pubsubCount()).append(" |\n");
        sb.append("| **matched** | **").append(r.matched()).append("** |\n");
        sb.append("| onlyLegacy | ").append(r.onlyLegacy().size()).append(" |\n");
        sb.append("| onlyPubsub | ").append(r.onlyPubsub().size()).append(" |\n");
        sb.append("| payloadMismatch | ").append(r.payloadMismatch().size()).append(" |\n");
        sb.append("| differences | ").append(r.differences()).append(" |\n\n");

        list(sb, "Only on legacy side (EMS/MQ → never reached Pub/Sub)", r.onlyLegacy());
        list(sb, "Only on Pub/Sub side (no legacy counterpart)", r.onlyPubsub());
        if (!r.payloadMismatch().isEmpty()) {
            sb.append("## Payload mismatches\n\n| orderId | legacy total | pubsub total | legacy lines | pubsub lines | reason |\n");
            sb.append("|---|---|---|---|---|---|\n");
            int n = 0;
            for (ReconciliationResult.PayloadMismatch m : r.payloadMismatch()) {
                if (n++ >= MAX_LISTED) {
                    sb.append("| … | | | | | ").append(r.payloadMismatch().size() - MAX_LISTED).append(" more |\n");
                    break;
                }
                sb.append("| ").append(m.orderId()).append(" | ").append(m.legacyTotalAmount()).append(" | ")
                        .append(m.pubsubTotalAmount()).append(" | ").append(m.legacyLineCount()).append(" | ")
                        .append(m.pubsubLineCount()).append(" | ").append(m.reason()).append(" |\n");
            }
            sb.append('\n');
        }
        sb.append("Go/no-go rule (RUNBOOK): differences must be 0 for 7 consecutive days in SHADOW and for the whole DUAL_RUN window.\n");
        return sb.toString();
    }

    private static void list(StringBuilder sb, String title, List<String> ids) {
        if (ids.isEmpty()) {
            return;
        }
        sb.append("## ").append(title).append(" (").append(ids.size()).append(")\n\n");
        int n = 0;
        for (String id : ids) {
            if (n++ >= MAX_LISTED) {
                sb.append("- … ").append(ids.size() - MAX_LISTED).append(" more\n");
                break;
            }
            sb.append("- `").append(id).append("`\n");
        }
        sb.append('\n');
    }
}
