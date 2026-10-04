package com.tailoredbrands.otd.migration.reconciler;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.tailoredbrands.otd.migration.common.jms.JmsConnectionFactories;
import com.tailoredbrands.otd.migration.common.jms.JmsSettings;
import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import com.tailoredbrands.otd.migration.reconciler.report.ReportWriter;
import com.tailoredbrands.otd.migration.reconciler.sink.BigQueryResultSink;
import com.tailoredbrands.otd.migration.reconciler.source.BigQueryOrderEventsSource;
import com.tailoredbrands.otd.migration.reconciler.source.FileRecordSource;
import com.tailoredbrands.otd.migration.reconciler.source.JmsAuditQueueSource;
import com.tailoredbrands.otd.migration.reconciler.source.RecordSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

/**
 * Orchestrates one run: pick the sources from the options, read both sides, reconcile, write
 * the JSON + Markdown report (stdout or {@code --out}), and optionally record the row in
 * BigQuery. Framework-free so it is unit-testable with files; Spring only parses the args.
 */
public class ReconcilerRunner {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerRunner.class);

    public static final int EXIT_OK = 0;
    public static final int EXIT_ERROR = 1;
    public static final int EXIT_DIFFERENCES = 2;

    private final JmsSettings jmsSettings;
    private final Supplier<BigQuery> bigQuerySupplier;
    private final PrintStream stdout;

    public ReconcilerRunner(JmsSettings jmsSettings) {
        this(jmsSettings, null, System.out);
    }

    public ReconcilerRunner(JmsSettings jmsSettings, Supplier<BigQuery> bigQuerySupplier, PrintStream stdout) {
        this.jmsSettings = jmsSettings;
        this.bigQuerySupplier = bigQuerySupplier;
        this.stdout = stdout;
    }

    /** Runs and returns the process exit code. */
    public int run(ReconcilerOptions options) {
        try {
            ReconciliationResult result = reconcile(options);
            if (options.failOnDiff() && !result.clean()) {
                return EXIT_DIFFERENCES;
            }
            return EXIT_OK;
        } catch (Exception e) {
            log.error("Reconciliation failed: {}", e.toString(), e);
            return EXIT_ERROR;
        }
    }

    public ReconciliationResult reconcile(ReconcilerOptions options) throws Exception {
        RecordSource legacy = legacySource(options);
        RecordSource pubsub = pubsubSource(options);
        log.info("Reconciling [{}, {}) phase={} legacy=<{}> pubsub=<{}>", options.from(), options.to(), options.phase(),
                legacy.describe(), pubsub.describe());

        List<SideRecord> legacyRecords = legacy.read(options.from(), options.to());
        List<SideRecord> pubsubRecords = pubsub.read(options.from(), options.to());

        ReconciliationResult result = new ReconciliationEngine(options.amountTolerance())
                .reconcile(legacyRecords, pubsubRecords, options.from(), options.to(), options.phase());

        ReportWriter writer = new ReportWriter();
        String json = writer.toJson(result);
        String markdown = writer.toMarkdown(result, legacy.describe(), pubsub.describe());
        write(options, json, markdown);

        if (options.usesBigQuery()) {
            new BigQueryResultSink(bigQuery(options), options.bigQueryProject(), options.bigQueryDataset(),
                    options.resultTable()).insert(result, json);
        }
        log.info("Reconciliation done: matched={} onlyLegacy={} onlyPubsub={} mismatched={}", result.matched(),
                result.onlyLegacy().size(), result.onlyPubsub().size(), result.payloadMismatch().size());
        return result;
    }

    private RecordSource legacySource(ReconcilerOptions options) {
        if (options.usesLegacyFile()) {
            return new FileRecordSource(Path.of(options.legacyFile()), "legacy");
        }
        return new JmsAuditQueueSource(JmsConnectionFactories.create(jmsSettings), options.legacyQueue());
    }

    private RecordSource pubsubSource(ReconcilerOptions options) {
        if (options.usesPubsubFile()) {
            return new FileRecordSource(Path.of(options.pubsubFile()), "pubsub");
        }
        if (!options.usesBigQuery()) {
            throw new IllegalArgumentException("Either --pubsubFile or --bigQueryDataset is required");
        }
        return new BigQueryOrderEventsSource(bigQuery(options), options.bigQueryProject(), options.bigQueryDataset(),
                options.bigQueryTable());
    }

    private BigQuery bigQuery(ReconcilerOptions options) {
        if (bigQuerySupplier != null) {
            return bigQuerySupplier.get();
        }
        BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();
        if (options.bigQueryProject() != null && !options.bigQueryProject().isBlank()) {
            builder.setProjectId(options.bigQueryProject());
        }
        return builder.build().getService();
    }

    private void write(ReconcilerOptions options, String json, String markdown) throws IOException {
        if (options.out() == null || options.out().isBlank()) {
            stdout.println(markdown);
            stdout.println(json);
            return;
        }
        Path base = Path.of(options.out());
        Path jsonPath = base.resolveSibling(base.getFileName() + ".json");
        Path mdPath = base.resolveSibling(base.getFileName() + ".md");
        if (base.getParent() != null) {
            Files.createDirectories(base.getParent());
        }
        Files.writeString(jsonPath, json, StandardCharsets.UTF_8);
        Files.writeString(mdPath, markdown, StandardCharsets.UTF_8);
        stdout.println(markdown);
        log.info("Report written to {} and {}", jsonPath, mdPath);
    }
}
