package com.tailoredbrands.otd.migration.common.headers;

import jakarta.jms.JMSException;
import jakarta.jms.Message;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Maps JMS headers / properties to Pub/Sub message attributes and back.
 *
 * <pre>
 *  JMS                      →  Pub/Sub attribute
 *  JMSMessageID                legacyMessageId      (dedup key downstream)
 *  JMSCorrelationID            correlationId
 *  JMSTimestamp                legacyTimestamp      (RFC-3339)
 *  JMSXDeliveryCount           legacyDeliveryCount
 *  JMSRedelivered              legacyRedelivered    ("true" only)
 *  property storeId            storeId              (also the ordering key)
 *  property eventType          eventType
 *  any other String property   copied as-is (lower precedence than the fixed ones)
 *
 *  Pub/Sub attribute        →  JMS
 *  correlationId               JMSCorrelationID
 *  storeId / eventType / eventId / source / schemaVersion / legacyMessageId   String properties
 *  pubsubMessageId             String property pubsubMessageId (set by pubsub-to-jms-bridge)
 * </pre>
 *
 * <p>JMS property names must be valid Java identifiers, so attribute names containing '-' or '.'
 * are skipped on the reverse path. Pub/Sub attributes are limited to 256-byte keys and 1024-byte
 * values; longer values are truncated.</p>
 */
public final class HeaderMapper {

    public static final String ATTR_LEGACY_MESSAGE_ID = "legacyMessageId";
    public static final String ATTR_CORRELATION_ID = "correlationId";
    public static final String ATTR_LEGACY_TIMESTAMP = "legacyTimestamp";
    public static final String ATTR_LEGACY_DELIVERY_COUNT = "legacyDeliveryCount";
    public static final String ATTR_LEGACY_REDELIVERED = "legacyRedelivered";
    public static final String ATTR_STORE_ID = "storeId";
    public static final String ATTR_EVENT_TYPE = "eventType";
    public static final String ATTR_EVENT_ID = "eventId";
    public static final String ATTR_SOURCE = "source";
    public static final String ATTR_SCHEMA_VERSION = "schemaVersion";
    public static final String ATTR_PUBSUB_MESSAGE_ID = "pubsubMessageId";

    public static final String PROP_STORE_ID = "storeId";
    public static final String PROP_EVENT_TYPE = "eventType";
    public static final String PROP_PUBSUB_MESSAGE_ID = "pubsubMessageId";
    public static final String PROP_JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    private static final int MAX_ATTRIBUTE_VALUE_BYTES = 1024;

    /** Attributes copied back to JMS String properties on the Pub/Sub → JMS path. */
    private static final Set<String> REVERSE_PROPERTY_ATTRIBUTES = Set.of(
            ATTR_STORE_ID, ATTR_EVENT_TYPE, ATTR_EVENT_ID, ATTR_SOURCE, ATTR_SCHEMA_VERSION,
            ATTR_LEGACY_MESSAGE_ID, ATTR_PUBSUB_MESSAGE_ID);

    private HeaderMapper() {
    }

    // ------------------------------------------------------------------ JMS -> Pub/Sub

    /** Reads headers and String properties from a JMS message and maps them to attributes. */
    public static Map<String, String> toAttributes(Message message) throws JMSException {
        Map<String, String> props = new LinkedHashMap<>();
        Enumeration<?> names = message.getPropertyNames();
        while (names != null && names.hasMoreElements()) {
            Object nameObj = names.nextElement();
            if (nameObj == null) {
                continue;
            }
            String name = nameObj.toString();
            Object value = message.getObjectProperty(name);
            if (value != null) {
                props.put(name, value.toString());
            }
        }
        long timestamp = message.getJMSTimestamp();
        return toAttributes(message.getJMSMessageID(), message.getJMSCorrelationID(),
                timestamp > 0 ? timestamp : null, message.getJMSRedelivered(), props);
    }

    /**
     * Pure mapping (no JMS API) so it can be unit tested without a broker.
     *
     * @param jmsMessageId    JMSMessageID
     * @param correlationId   JMSCorrelationID
     * @param jmsTimestampMs  JMSTimestamp in epoch millis, or null when unset
     * @param redelivered     JMSRedelivered
     * @param properties      String-ified JMS properties
     */
    public static Map<String, String> toAttributes(String jmsMessageId, String correlationId, Long jmsTimestampMs,
                                                   boolean redelivered, Map<String, String> properties) {
        Map<String, String> attrs = new LinkedHashMap<>();
        if (properties != null) {
            for (Map.Entry<String, String> e : properties.entrySet()) {
                String name = e.getKey();
                String value = e.getValue();
                if (name == null || value == null || value.isEmpty()) {
                    continue;
                }
                if (PROP_JMSX_DELIVERY_COUNT.equals(name)) {
                    attrs.put(ATTR_LEGACY_DELIVERY_COUNT, value);
                } else if (name.startsWith("JMSX") || name.startsWith("JMS_")) {
                    // provider-specific headers (JMS_IBM_*, JMS_TIBCO_*) are noise for Pub/Sub consumers
                    continue;
                } else {
                    attrs.put(name, truncate(value));
                }
            }
        }
        if (jmsMessageId != null && !jmsMessageId.isBlank()) {
            attrs.put(ATTR_LEGACY_MESSAGE_ID, jmsMessageId);
        }
        if (correlationId != null && !correlationId.isBlank()) {
            attrs.put(ATTR_CORRELATION_ID, correlationId);
        }
        if (jmsTimestampMs != null) {
            attrs.put(ATTR_LEGACY_TIMESTAMP, Instant.ofEpochMilli(jmsTimestampMs).toString());
        }
        if (redelivered) {
            attrs.put(ATTR_LEGACY_REDELIVERED, "true");
        }
        return attrs;
    }

    /** Extracts the JMSTimestamp mapped into attributes, if any. */
    public static Instant legacyTimestamp(Map<String, String> attributes) {
        String ts = attributes == null ? null : attributes.get(ATTR_LEGACY_TIMESTAMP);
        if (ts == null) {
            return null;
        }
        try {
            return Instant.parse(ts);
        } catch (DateTimeParseException e) {
            try {
                return Instant.ofEpochMilli(Long.parseLong(ts));
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
    }

    // ------------------------------------------------------------------ Pub/Sub -> JMS

    /**
     * Applies Pub/Sub attributes to an outgoing JMS message: {@code correlationId} becomes the
     * JMSCorrelationID, the well-known attributes become String properties.
     *
     * @param pubsubMessageId the Pub/Sub message id, stored as property {@code pubsubMessageId}
     */
    public static void applyToMessage(Message message, Map<String, String> attributes, String pubsubMessageId)
            throws JMSException {
        Map<String, String> props = toJmsProperties(attributes, pubsubMessageId);
        String correlationId = attributes == null ? null : attributes.get(ATTR_CORRELATION_ID);
        if (correlationId != null && !correlationId.isBlank()) {
            message.setJMSCorrelationID(correlationId);
        }
        for (Map.Entry<String, String> e : props.entrySet()) {
            message.setStringProperty(e.getKey(), e.getValue());
        }
    }

    /** Pure reverse mapping: attributes → JMS String properties (JMSCorrelationID handled separately). */
    public static Map<String, String> toJmsProperties(Map<String, String> attributes, String pubsubMessageId) {
        Map<String, String> props = new LinkedHashMap<>();
        if (attributes != null) {
            for (Map.Entry<String, String> e : attributes.entrySet()) {
                String name = e.getKey();
                String value = e.getValue();
                if (value == null || value.isEmpty() || !REVERSE_PROPERTY_ATTRIBUTES.contains(name)) {
                    continue;
                }
                if (isValidJmsPropertyName(name)) {
                    props.put(name, value);
                }
            }
        }
        if (pubsubMessageId != null && !pubsubMessageId.isBlank()) {
            props.put(PROP_PUBSUB_MESSAGE_ID, pubsubMessageId);
        }
        return props;
    }

    static boolean isValidJmsPropertyName(String name) {
        if (name == null || name.isEmpty() || !Character.isJavaIdentifierStart(name.charAt(0))) {
            return false;
        }
        for (int i = 1; i < name.length(); i++) {
            if (!Character.isJavaIdentifierPart(name.charAt(i))) {
                return false;
            }
        }
        return !name.startsWith("JMS");
    }

    private static String truncate(String value) {
        if (value.length() <= MAX_ATTRIBUTE_VALUE_BYTES / 4) {
            return value; // definitely under the byte limit even for 4-byte UTF-8 code points
        }
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length <= MAX_ATTRIBUTE_VALUE_BYTES) {
            return value;
        }
        // cut on a char boundary that stays under the byte limit
        String cut = value;
        while (cut.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_ATTRIBUTE_VALUE_BYTES) {
            cut = cut.substring(0, cut.length() - 1);
        }
        return cut;
    }
}
