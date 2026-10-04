package com.tailoredbrands.otd.migration.reconciler.source;

import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;

import java.time.Instant;
import java.util.List;

/** A side of the reconciliation (legacy or Pub/Sub). Implementations filter to the window. */
public interface RecordSource {

    /** Human readable description for the report, e.g. "JMS browse TB.ORDERS.AUDIT" or "BigQuery otd.order_events". */
    String describe();

    List<SideRecord> read(Instant from, Instant to) throws Exception;
}
