package com.tailoredbrands.otd.migration.reconciler.source;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.TableResult;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pub/Sub side via BigQuery {@code otd.order_events} (written by the Dataflow streaming job:
 * flattened envelope + order header, {@code lines} as a REPEATED RECORD). One row per order,
 * the first ORDER_CREATED event in the window.
 */
public class BigQueryOrderEventsSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(BigQueryOrderEventsSource.class);

    private final BigQuery bigQuery;
    private final String project;
    private final String dataset;
    private final String table;

    public BigQueryOrderEventsSource(BigQuery bigQuery, String project, String dataset, String table) {
        this.bigQuery = bigQuery;
        this.project = project;
        this.dataset = dataset;
        this.table = table;
    }

    @Override
    public String describe() {
        return "BigQuery " + qualifiedTable();
    }

    String qualifiedTable() {
        return (project == null || project.isBlank() ? "" : project + ".") + dataset + "." + table;
    }

    String sql() {
        return "SELECT order_id, "
                + "  ANY_VALUE(total_amount) AS total_amount, "
                + "  ANY_VALUE(ARRAY_LENGTH(lines)) AS line_count, "
                + "  MIN(event_time) AS event_time, "
                + "  ANY_VALUE(COALESCE(legacy_message_id, event_id)) AS message_id "
                + "FROM `" + qualifiedTable() + "` "
                + "WHERE event_time >= @from_ts AND event_time < @to_ts "
                + "  AND event_type = 'ORDER_CREATED' "
                + "GROUP BY order_id";
    }

    @Override
    public List<SideRecord> read(Instant from, Instant to) throws InterruptedException {
        QueryJobConfiguration query = QueryJobConfiguration.newBuilder(sql())
                .addNamedParameter("from_ts", QueryParameterValue.timestamp(micros(from)))
                .addNamedParameter("to_ts", QueryParameterValue.timestamp(micros(to)))
                .setUseLegacySql(false)
                .build();
        TableResult result = bigQuery.query(query);
        List<SideRecord> out = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            String orderId = string(row.get("order_id"));
            if (orderId == null) {
                continue;
            }
            FieldValue total = row.get("total_amount");
            FieldValue lines = row.get("line_count");
            FieldValue eventTime = row.get("event_time");
            out.add(new SideRecord(
                    orderId,
                    total.isNull() ? null : new BigDecimal(total.getStringValue()),
                    lines.isNull() ? null : (int) lines.getLongValue(),
                    eventTime.isNull() ? null : Instant.ofEpochMilli(eventTime.getTimestampValue() / 1000),
                    string(row.get("message_id"))));
        }
        log.info("{}: {} orders in window [{}, {})", describe(), out.size(), from, to);
        return out;
    }

    private static String string(FieldValue v) {
        return v == null || v.isNull() ? null : v.getStringValue();
    }

    private static long micros(Instant i) {
        return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000L;
    }
}
