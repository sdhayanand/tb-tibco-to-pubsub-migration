package com.tailoredbrands.otd.migration.jmsbridge;

import com.tailoredbrands.otd.migration.common.headers.HeaderMapper;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlException;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import io.micrometer.core.instrument.Timer;
import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

/**
 * One legacy JMS message in, one canonical Pub/Sub message out.
 *
 * <ol>
 *   <li>Read the XML body (TextMessage, or BytesMessage as UTF-8).</li>
 *   <li>Map JMS headers → attributes ({@link HeaderMapper}); add {@code source=TIBCO_EMS_BRIDGE},
 *       {@code eventType}, {@code eventId}, {@code schemaVersion}, {@code storeId}.</li>
 *   <li>XML → {@link OrderEvent} JSON ({@link LegacyXmlMapper}).</li>
 *   <li>Publish with ordering key = storeId and <b>wait for the Pub/Sub ack</b>.</li>
 *   <li>Return normally → the transacted JMS session commits (message consumed).
 *       Throw → rollback → the broker redelivers (at-least-once; consumers dedup on
 *       {@code legacyMessageId}).</li>
 * </ol>
 * Unparseable (poison) messages are published to {@code events-dlq} with {@code dlqReason} and
 * then committed, so one bad message never blocks the queue. Publish failures of a message
 * that has already been redelivered more than {@code bridge.max-delivery-attempts} times are
 * also routed to the DLQ (otherwise the broker would keep redelivering forever).
 */
@Component
public class LegacyOrderMessageListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(LegacyOrderMessageListener.class);
    static final String DLQ_STAGE = "jms-to-pubsub-bridge";
    /** Pub/Sub accepts 10 MB per message; leave room for attributes. Bigger payloads need a claim-check (GCS). */
    static final int MAX_PAYLOAD_BYTES = 9_000_000;

    private final LegacyXmlMapper xmlMapper;
    private final OrdersPublisher publisher;
    private final BridgeMetrics metrics;
    private final BridgeProperties props;

    public LegacyOrderMessageListener(LegacyXmlMapper xmlMapper, OrdersPublisher publisher, BridgeMetrics metrics,
                                      BridgeProperties props) {
        this.xmlMapper = xmlMapper;
        this.publisher = publisher;
        this.metrics = metrics;
        this.props = props;
    }

    @Override
    public void onMessage(Message message) {
        String payload = null;
        Map<String, String> attributes = null;
        String messageId = null;
        try {
            messageId = message.getJMSMessageID();
            MDC.put("legacyMessageId", messageId);
            attributes = HeaderMapper.toAttributes(message);
            if (message.getJMSRedelivered()) {
                metrics.recordDuplicate();
            }
            payload = body(message);
            Timer.Sample sample = Timer.start();
            String pubsubId = bridge(payload, attributes, message);
            sample.stop(metrics.publishTimer());
            metrics.recordBridged(HeaderMapper.legacyTimestamp(attributes));
            log.info("Bridged JMS {} -> Pub/Sub {} (orderId={}, storeId={})", messageId, pubsubId,
                    MDC.get("orderId"), attributes.get(HeaderMapper.ATTR_STORE_ID));
        } catch (LegacyXmlException | UnsupportedMessageException e) {
            // poison: cannot ever succeed → DLQ, then commit
            log.warn("Poison message {} -> {}: {}", messageId, props.dlqTopic(), e.getMessage());
            publisher.publishDlq(payload, attributes, e.getMessage(), DLQ_STAGE);
            metrics.recordDlq();
        } catch (OrdersPublisher.PublishException e) {
            metrics.recordFailure("publish");
            int attempts = deliveryCount(message);
            if (attempts > props.maxDeliveryAttempts()) {
                log.error("Message {} failed to publish after {} deliveries; routing to {}", messageId, attempts,
                        props.dlqTopic(), e);
                publisher.publishDlq(payload, attributes, "publish failed after " + attempts + " deliveries: "
                        + e.getMessage(), DLQ_STAGE);
                metrics.recordDlq();
                return;
            }
            log.error("Publish failed for {} (delivery {}), rolling back for redelivery: {}", messageId, attempts,
                    e.getMessage());
            throw e; // rollback → redelivery
        } catch (JMSException e) {
            metrics.recordFailure("jms");
            throw new IllegalStateException("JMS error while reading " + messageId + ": " + e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    private String bridge(String xml, Map<String, String> attributes, Message message) throws JMSException {
        String eventType = attributes.getOrDefault(HeaderMapper.ATTR_EVENT_TYPE, props.defaultEventType());
        Instant eventTime = HeaderMapper.legacyTimestamp(attributes);
        OrderEvent event = xmlMapper.toEvent(xml, eventType, eventTime,
                attributes.get(HeaderMapper.ATTR_CORRELATION_ID), message.getJMSMessageID());
        MDC.put("orderId", event.order().orderId());
        MDC.put("eventId", event.eventId());
        if (event.correlationId() != null) {
            MDC.put("correlationId", event.correlationId());
        }

        // attributes required by ARCHITECTURE.md section 3.1
        attributes.put(HeaderMapper.ATTR_EVENT_TYPE, event.eventType());
        attributes.put(HeaderMapper.ATTR_SCHEMA_VERSION, event.schemaVersion());
        attributes.put(HeaderMapper.ATTR_SOURCE, OrderEvent.SOURCE_TIBCO_EMS_BRIDGE);
        attributes.put(HeaderMapper.ATTR_STORE_ID, event.order().storeId());
        attributes.put(HeaderMapper.ATTR_EVENT_ID, event.eventId());
        if (event.correlationId() != null) {
            attributes.put(HeaderMapper.ATTR_CORRELATION_ID, event.correlationId());
        }
        if (event.legacyMessageId() != null) {
            attributes.put(HeaderMapper.ATTR_LEGACY_MESSAGE_ID, event.legacyMessageId());
        }

        String json = xmlMapper.toJson(event);
        return publisher.publishOrdered(json, attributes, event.order().storeId());
    }

    static String body(Message message) throws JMSException {
        if (message instanceof TextMessage text) {
            String s = text.getText();
            if (s == null || s.isBlank()) {
                throw new LegacyXmlException("Empty TextMessage body");
            }
            if (s.length() > MAX_PAYLOAD_BYTES) {
                throw new UnsupportedMessageException("Payload of " + s.length() + " chars exceeds the Pub/Sub 10 MB limit; "
                        + "use the claim-check pattern (GCS) for this destination");
            }
            return s;
        }
        if (message instanceof BytesMessage bytes) {
            long length = bytes.getBodyLength();
            if (length <= 0 || length > MAX_PAYLOAD_BYTES) {
                throw new UnsupportedMessageException("BytesMessage body length " + length + " not supported (max "
                        + MAX_PAYLOAD_BYTES + " bytes; use the claim-check pattern for bigger payloads)");
            }
            byte[] buf = new byte[(int) length];
            bytes.readBytes(buf);
            return new String(buf, StandardCharsets.UTF_8);
        }
        throw new UnsupportedMessageException("Unsupported JMS message type " + message.getClass().getName()
                + " (expected TextMessage or BytesMessage)");
    }

    static int deliveryCount(Message message) {
        try {
            if (message.propertyExists(HeaderMapper.PROP_JMSX_DELIVERY_COUNT)) {
                return message.getIntProperty(HeaderMapper.PROP_JMSX_DELIVERY_COUNT);
            }
            return message.getJMSRedelivered() ? 2 : 1;
        } catch (JMSException | RuntimeException e) {
            return 1;
        }
    }

    /** A JMS message type the bridge cannot convert (MapMessage, ObjectMessage, StreamMessage). */
    public static class UnsupportedMessageException extends RuntimeException {
        public UnsupportedMessageException(String message) {
            super(message);
        }
    }
}
