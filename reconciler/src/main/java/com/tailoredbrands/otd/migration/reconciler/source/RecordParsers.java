package com.tailoredbrands.otd.migration.reconciler.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.tailoredbrands.otd.migration.common.model.Order;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tolerant field extraction shared by the file and BigQuery sources. Accepts the canonical
 * lowerCamelCase names, BigQuery snake_case names and the legacy XML element names.
 */
final class RecordParsers {

    static final List<String> ORDER_ID = List.of("orderId", "order_id", "OrderNbr", "orderNbr", "order_nbr");
    static final List<String> TOTAL = List.of("totalAmount", "total_amount", "TotalAmt", "totalAmt", "total");
    static final List<String> LINE_COUNT = List.of("lineCount", "line_count", "lines", "numLines", "line_cnt");
    static final List<String> TIMESTAMP = List.of("timestamp", "eventTime", "event_time", "orderedAt", "ordered_at",
            "OrderDate", "jmsTimestamp", "jms_timestamp", "publish_time", "publishTime");
    static final List<String> MESSAGE_ID = List.of("messageId", "message_id", "legacyMessageId", "legacy_message_id",
            "jmsMessageId", "JMSMessageID", "eventId", "event_id");

    private RecordParsers() {
    }

    /** A canonical {@code OrderEvent} JSON object (has an "order" child) or a flat row. */
    static SideRecord fromJson(JsonNode node, LegacyXmlMapper xmlMapper) {
        if (node.hasNonNull("xml")) {
            Order order = xmlMapper.parseOrder(node.get("xml").asText());
            Instant ts = instant(firstText(node, TIMESTAMP));
            return new SideRecord(order.orderId(), order.totalAmount(), order.lineCount(),
                    ts == null ? order.orderedAt() : ts, firstText(node, MESSAGE_ID));
        }
        if (node.hasNonNull("order") && node.get("order").isObject()) {
            JsonNode order = node.get("order");
            Integer lines = order.hasNonNull("lines") && order.get("lines").isArray() ? order.get("lines").size() : null;
            String messageId = firstText(node, List.of("legacyMessageId", "eventId"));
            return new SideRecord(firstText(order, ORDER_ID), decimal(firstText(order, TOTAL)), lines,
                    instant(firstText(node, List.of("eventTime", "event_time", "publish_time"))), messageId);
        }
        Integer lines;
        JsonNode linesNode = node.get("lines");
        if (linesNode != null && linesNode.isArray()) {
            lines = linesNode.size();
        } else {
            lines = integer(firstText(node, LINE_COUNT));
        }
        return new SideRecord(firstText(node, ORDER_ID), decimal(firstText(node, TOTAL)), lines,
                instant(firstText(node, TIMESTAMP)), firstText(node, MESSAGE_ID));
    }

    /** A CSV row already split into header → value. */
    static SideRecord fromRow(Map<String, String> row) {
        return new SideRecord(first(row, ORDER_ID), decimal(first(row, TOTAL)), integer(first(row, LINE_COUNT)),
                instant(first(row, TIMESTAMP)), first(row, MESSAGE_ID));
    }

    static String firstText(JsonNode node, List<String> names) {
        for (String n : names) {
            JsonNode v = node.get(n);
            if (v != null && !v.isNull()) {
                if (v.isArray() || v.isObject()) {
                    continue;
                }
                String s = v.asText();
                if (!s.isBlank()) {
                    return s;
                }
            }
        }
        return null;
    }

    static String first(Map<String, String> row, List<String> names) {
        for (String n : names) {
            for (Map.Entry<String, String> e : row.entrySet()) {
                if (e.getKey().equalsIgnoreCase(n) && e.getValue() != null && !e.getValue().isBlank()) {
                    return e.getValue().trim();
                }
            }
        }
        return null;
    }

    static BigDecimal decimal(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return new BigDecimal(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static Integer integer(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** RFC-3339, ISO local date-time (UTC), "yyyy-MM-dd HH:mm:ss[.SSSSSS] [UTC]" (BigQuery export) or epoch millis. */
    static Instant instant(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        String v = s.trim();
        if (v.endsWith(" UTC")) {
            v = v.substring(0, v.length() - 4);
        }
        try {
            return Instant.parse(v);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDateTime.parse(v.replace(' ', 'T')).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            long n = Long.parseLong(v);
            // BigQuery exports TIMESTAMP as micros in some paths, JMS uses millis
            return n > 100_000_000_000_000L ? Instant.ofEpochMilli(n / 1000) : Instant.ofEpochMilli(n);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    /** Minimal RFC-4180 line splitter (quotes, escaped quotes, no embedded newlines). */
    static List<String> splitCsv(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cur.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }

    static String normalizeHeader(String h) {
        return h == null ? "" : h.trim().replace("﻿", "").toLowerCase(Locale.ROOT);
    }
}
