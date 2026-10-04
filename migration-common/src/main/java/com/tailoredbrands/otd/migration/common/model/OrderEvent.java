package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.UUID;

/**
 * Canonical event envelope published on {@code orders-v1} (ARCHITECTURE.md section 3.1).
 * Field names are lowerCamelCase to match the proto JSON mapping of the Pub/Sub schema.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderEvent(
        String eventId,
        String eventType,
        Instant eventTime,
        String schemaVersion,
        String source,
        String correlationId,
        String legacyMessageId,
        Order order) {

    public static final String SCHEMA_VERSION = "1";

    public static final String TYPE_ORDER_CREATED = "ORDER_CREATED";
    public static final String TYPE_ORDER_UPDATED = "ORDER_UPDATED";
    public static final String TYPE_ORDER_CANCELLED = "ORDER_CANCELLED";

    public static final String SOURCE_ORDER_INTAKE_API = "ORDER_INTAKE_API";
    public static final String SOURCE_LEGACY_SOAP_ADAPTER = "LEGACY_SOAP_ADAPTER";
    public static final String SOURCE_TIBCO_EMS_BRIDGE = "TIBCO_EMS_BRIDGE";
    public static final String SOURCE_REPLAY = "REPLAY";

    /**
     * Builds an envelope for an order that was bridged from a legacy JMS message.
     *
     * @param order           the parsed order
     * @param eventType       ORDER_CREATED | ORDER_UPDATED | ORDER_CANCELLED
     * @param eventTime       usually the JMSTimestamp, falling back to now
     * @param correlationId   JMSCorrelationID (may be null)
     * @param legacyMessageId JMSMessageID (used for de-duplication downstream)
     */
    public static OrderEvent bridged(Order order, String eventType, Instant eventTime,
                                     String correlationId, String legacyMessageId) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                eventType == null ? TYPE_ORDER_CREATED : eventType,
                eventTime == null ? Instant.now() : eventTime,
                SCHEMA_VERSION,
                SOURCE_TIBCO_EMS_BRIDGE,
                correlationId,
                legacyMessageId,
                order);
    }

    /** Not named like a bean getter on purpose: it must not become a JSON property. */
    public boolean cameFromLegacyBridge() {
        return SOURCE_TIBCO_EMS_BRIDGE.equals(source);
    }
}
