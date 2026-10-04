package com.tailoredbrands.otd.migration.reconciler.sink;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.tailoredbrands.otd.migration.reconciler.model.ReconciliationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Streams one row per run into {@code otd.migration_reconciliation}
 * (Terraform {@code modules/bigquery}): {@code run_time, from_ts, to_ts, phase, matched,
 * only_legacy, only_pubsub, mismatched, details} — {@code details} is the JSON report (STRING/JSON column).
 * A Cloud Monitoring alert on {@code only_legacy + only_pubsub + mismatched > 0} is the
 * reconciler's "page me" signal during SHADOW / DUAL_RUN.
 */
public class BigQueryResultSink {

    private static final Logger log = LoggerFactory.getLogger(BigQueryResultSink.class);

    private final BigQuery bigQuery;
    private final TableId table;

    public BigQueryResultSink(BigQuery bigQuery, String project, String dataset, String table) {
        this.bigQuery = bigQuery;
        this.table = project == null || project.isBlank() ? TableId.of(dataset, table) : TableId.of(project, dataset, table);
    }

    public void insert(ReconciliationResult result, String detailsJson) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("run_time", result.runTime().toString());
        row.put("from_ts", result.from().toString());
        row.put("to_ts", result.to().toString());
        row.put("phase", result.phase());
        row.put("matched", result.matched());
        row.put("only_legacy", result.onlyLegacy().size());
        row.put("only_pubsub", result.onlyPubsub().size());
        row.put("mismatched", result.payloadMismatch().size());
        row.put("details", detailsJson);
        InsertAllRequest request = InsertAllRequest.newBuilder(table)
                .addRow(UUID.randomUUID().toString(), row)
                .build();
        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Long, List<BigQueryError>> e : response.getInsertErrors().entrySet()) {
                sb.append(e.getKey()).append(": ").append(e.getValue()).append("; ");
            }
            throw new IllegalStateException("BigQuery insert into " + table + " failed: " + sb);
        }
        log.info("Inserted reconciliation row into {} (matched={}, diff={})", table, result.matched(), result.differences());
    }
}
