package com.tailoredbrands.otd.migration.reconciler.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The minimal projection of an order on either side of the migration. The join key is
 * {@code orderId}; {@code totalAmount} and {@code lineCount} are the payload fields compared.
 *
 * @param orderId     order number (OrderNbr / order.orderId)
 * @param totalAmount order total (TotalAmt / order.totalAmount), may be null
 * @param lineCount   number of lines, may be null
 * @param timestamp   JMSTimestamp / OrderDate on the legacy side, event_time on the Pub/Sub side
 * @param messageId   JMSMessageID / legacy_message_id or eventId, informational
 */
public record SideRecord(String orderId, BigDecimal totalAmount, Integer lineCount, Instant timestamp,
                         String messageId) {

    public boolean inWindow(Instant from, Instant to) {
        if (timestamp == null) {
            return true; // no timestamp → cannot exclude it
        }
        return (from == null || !timestamp.isBefore(from)) && (to == null || timestamp.isBefore(to));
    }
}
