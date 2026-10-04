package com.tailoredbrands.otd.migration.pubsubbridge;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.tailoredbrands.otd.migration.common.headers.HeaderMapper;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlException;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One canonical Pub/Sub message in, one legacy JMS message out.
 *
 * <ol>
 *   <li><b>Loop guard</b>: events whose {@code source} attribute (or payload field) is
 *       {@code TIBCO_EMS_BRIDGE} came from the other bridge and must not go back to the legacy
 *       side. The subscription filter {@code attributes.source != "TIBCO_EMS_BRIDGE"} already
 *       drops them server-side; this is belt and braces (and covers the emulator, which may
 *       ignore filters).</li>
 *   <li>JSON → {@link OrderEvent} → legacy XML.</li>
 *   <li>Transacted JMS send ({@link LegacyJmsSender}), then {@code ack()}.</li>
 *   <li>Unparseable payloads go to {@code events-dlq} and are acked. Send failures are
 *       {@code nack()}ed for redelivery until {@code delivery_attempt} reaches
 *       {@code bridge.delivery-attempts-before-dlq}, then DLQ + ack.</li>
 * </ol>
 */
@Component
public class CanonicalEventReceiver implements MessageReceiver {

    private static final Logger log = LoggerFactory.getLogger(CanonicalEventReceiver.class);
    static final String DLQ_STAGE = "pubsub-to-jms-bridge";

    private final LegacyXmlMapper xmlMapper;
    private final LegacyJmsSender sender;
    private final DlqPublisher dlq;
    private final BridgeMetrics metrics;
    private final BridgeProperties props;

    public CanonicalEventReceiver(LegacyXmlMapper xmlMapper, LegacyJmsSender sender, DlqPublisher dlq,
                                  BridgeMetrics metrics, BridgeProperties props) {
        this.xmlMapper = xmlMapper;
        this.sender = sender;
        this.dlq = dlq;
        this.metrics = metrics;
        this.props = props;
    }

    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        Map<String, String> attributes = new LinkedHashMap<>(message.getAttributesMap());
        String messageId = message.getMessageId();
        String payload = message.getData().toStringUtf8();
        Integer attempt = Subscriber.getDeliveryAttempt(message);
        try {
            MDC.put("pubsubMessageId", messageId);
            if (attributes.containsKey(HeaderMapper.ATTR_CORRELATION_ID)) {
                MDC.put("correlationId", attributes.get(HeaderMapper.ATTR_CORRELATION_ID));
            }
            if (props.loopGuardSource().equals(attributes.get(HeaderMapper.ATTR_SOURCE))) {
                skip(messageId, consumer, "attribute source=" + props.loopGuardSource());
                return;
            }
            if (attempt != null && attempt > 1) {
                metrics.recordDuplicate();
            }

            OrderEvent event = xmlMapper.fromJson(payload);
            if (props.loopGuardSource().equals(event.source())) {
                skip(messageId, consumer, "payload source=" + props.loopGuardSource());
                return;
            }
            MDC.put("orderId", event.order().orderId());
            MDC.put("eventId", event.eventId());

            String xml = xmlMapper.toXml(event);
            Timer.Sample sample = Timer.start();
            sender.send(xml, event, attributes, messageId);
            sample.stop(metrics.publishTimer());
            consumer.ack();
            metrics.recordBridged(event.eventTime());
            log.info("Bridged Pub/Sub {} -> JMS {} (orderId={}, eventType={}, attempt={})", messageId,
                    props.destination(), event.order().orderId(), event.eventType(), attempt);
        } catch (LegacyXmlException e) {
            log.warn("Poison event {} -> {}: {}", messageId, props.dlqTopic(), e.getMessage());
            toDlq(payload, attributes, e.getMessage(), consumer);
        } catch (RuntimeException e) {
            metrics.recordFailure("jms-send");
            if (attempt != null && attempt >= props.deliveryAttemptsBeforeDlq()) {
                log.error("Event {} failed on delivery attempt {}; routing to {}", messageId, attempt, props.dlqTopic(), e);
                toDlq(payload, attributes, "send failed after " + attempt + " attempts: " + e.getMessage(), consumer);
                return;
            }
            log.error("JMS send failed for {} (attempt {}), nack for redelivery: {}", messageId, attempt, e.toString());
            consumer.nack();
        } finally {
            MDC.clear();
        }
    }

    private void skip(String messageId, AckReplyConsumer consumer, String why) {
        metrics.recordSkipped();
        consumer.ack();
        log.debug("Loop guard: skipped {} ({})", messageId, why);
    }

    private void toDlq(String payload, Map<String, String> attributes, String reason, AckReplyConsumer consumer) {
        try {
            dlq.publish(payload, attributes, reason, DLQ_STAGE, props.sourceSubscription());
            metrics.recordDlq();
            consumer.ack();
        } catch (RuntimeException dlqFailure) {
            // Pub/Sub itself is unhappy: leave the message for redelivery rather than losing it
            metrics.recordFailure("dlq-publish");
            log.error("Could not publish to {}: {}", props.dlqTopic(), dlqFailure.toString());
            consumer.nack();
        }
    }
}
