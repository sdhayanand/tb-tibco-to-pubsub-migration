package com.tailoredbrands.otd.migration.reconciler;

import com.fasterxml.jackson.databind.JsonNode;
import com.tailoredbrands.otd.migration.common.jms.JmsSettings;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

class ReconcilerRunnerTest {

    static String resource(String name) throws Exception {
        return Paths.get(ReconcilerRunnerTest.class.getClassLoader().getResource(name).toURI()).toString();
    }

    private static ReconcilerRunner runner(ByteArrayOutputStream out) {
        JmsSettings jms = new JmsSettings("artemis", "tcp://unused:61616", null, null, null);
        return new ReconcilerRunner(jms, null, new PrintStream(out, true, StandardCharsets.UTF_8));
    }

    @Test
    void reconcilesFilesAndWritesJsonAndMarkdownReports(@TempDir Path tmp) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ReconcilerOptions options = ReconcilerOptions.parse(new String[] {
                "--from=2026-10-03T00:00:00Z", "--to=2026-10-04T00:00:00Z", "--phase=SHADOW",
                "--legacyFile=" + resource("legacy-orders.csv"),
                "--pubsubFile=" + resource("pubsub-events.jsonl"),
                "--out=" + tmp.resolve("reports/recon")});

        ReconciliationResult result = runner(out).reconcile(options);

        assertThat(result.legacyCount()).isEqualTo(5);  // duplicate ORD-...105 collapsed, 199 outside window
        assertThat(result.pubsubCount()).isEqualTo(5);
        assertThat(result.matched()).isEqualTo(2);
        assertThat(result.onlyLegacy()).containsExactly("ORD-2026-000105");
        assertThat(result.onlyPubsub()).containsExactly("ORD-2026-000106");
        assertThat(result.payloadMismatch()).extracting(ReconciliationResult.PayloadMismatch::orderId)
                .containsExactly("ORD-2026-000103", "ORD-2026-000104");
        assertThat(result.payloadMismatch().get(0).reason()).contains("lineCount 3 != 2");
        assertThat(result.payloadMismatch().get(1).reason()).contains("totalAmount 89.99 != 89.95");

        Path json = tmp.resolve("reports/recon.json");
        Path md = tmp.resolve("reports/recon.md");
        assertThat(json).exists();
        assertThat(md).exists();
        JsonNode node = JsonMappers.canonical().readTree(Files.readString(json));
        assertThat(node.get("matched").asInt()).isEqualTo(2);
        assertThat(node.get("phase").asText()).isEqualTo("SHADOW");
        assertThat(node.get("onlyLegacy").get(0).asText()).isEqualTo("ORD-2026-000105");
        String markdown = Files.readString(md);
        assertThat(markdown).contains("DIFFERENCES").contains("| **matched** | **2** |")
                .contains("ORD-2026-000106").contains("lineCount 3 != 2");
        assertThat(out.toString(StandardCharsets.UTF_8)).contains("# Migration reconciliation");
    }

    @Test
    void exitCodesReflectDifferencesAndErrors() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String[] base = {"--from=2026-10-03", "--to=2026-10-04",
                "--legacyFile=" + resource("legacy-orders.csv"), "--pubsubFile=" + resource("pubsub-events.jsonl")};

        assertThat(runner(out).run(ReconcilerOptions.parse(base))).isEqualTo(ReconcilerRunner.EXIT_OK);

        String[] strict = new String[base.length + 1];
        System.arraycopy(base, 0, strict, 0, base.length);
        strict[base.length] = "--failOnDiff=true";
        assertThat(runner(out).run(ReconcilerOptions.parse(strict))).isEqualTo(ReconcilerRunner.EXIT_DIFFERENCES);

        ReconcilerOptions missing = ReconcilerOptions.parse(new String[] {"--from=2026-10-03", "--to=2026-10-04",
                "--legacyFile=/nonexistent/legacy.csv", "--pubsubFile=" + resource("pubsub-events.jsonl")});
        assertThat(runner(out).run(missing)).isEqualTo(ReconcilerRunner.EXIT_ERROR);

        ReconcilerOptions noPubsub = ReconcilerOptions.parse(new String[] {"--from=2026-10-03", "--to=2026-10-04",
                "--legacyFile=" + resource("legacy-orders.csv")});
        assertThat(runner(out).run(noPubsub)).isEqualTo(ReconcilerRunner.EXIT_ERROR);
    }

    @Test
    void cleanWindowIsClean() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        // same file on both sides → everything matches
        ReconcilerOptions options = ReconcilerOptions.parse(new String[] {
                "--from=2026-10-03", "--to=2026-10-04", "--phase=DUAL_RUN",
                "--legacyFile=" + resource("legacy-orders.csv"), "--pubsubFile=" + resource("legacy-orders.csv")});
        ReconciliationResult result = runner(out).reconcile(options);
        assertThat(result.clean()).isTrue();
        assertThat(result.matched()).isEqualTo(5);
        assertThat(out.toString(StandardCharsets.UTF_8)).contains("CLEAN");
    }
}
