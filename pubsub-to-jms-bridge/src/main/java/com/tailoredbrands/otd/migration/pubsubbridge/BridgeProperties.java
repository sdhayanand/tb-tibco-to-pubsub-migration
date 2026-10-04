package com.tailoredbrands.otd.migration.pubsubbridge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code bridge.*} properties (see application.yml for the env var mapping).
 *
 * @param sourceSubscription SOURCE_SUBSCRIPTION (orders-to-legacy-mq)
 * @param destination        JMS_DESTINATION (ERP.ORDERS.IN). For IBM MQ with a non-JMS consumer use
 *                           the URI form {@code queue:///ERP.ORDERS.IN?targetClient=1} to suppress the
 *                           MQRFH2 header.
 * @param destinationType    JMS_DESTINATION_TYPE queue | topic
 * @param dlqTopic           DLQ_TOPIC (events-dlq) for unparseable / unconvertible events
 * @param flowControlMessages max outstanding Pub/Sub messages (CONVENTIONS: 100)
 * @param loopGuardSource    events with this {@code source} attribute are acked and skipped
 * @param deliveryAttemptsBeforeDlq when Pub/Sub delivery_attempt reaches this, a failing message goes to the DLQ
 */
@ConfigurationProperties(prefix = "bridge")
public record BridgeProperties(
        @DefaultValue("orders-to-legacy-mq") String sourceSubscription,
        @DefaultValue("ERP.ORDERS.IN") String destination,
        @DefaultValue("queue") String destinationType,
        @DefaultValue("events-dlq") String dlqTopic,
        @DefaultValue("100") long flowControlMessages,
        @DefaultValue("TIBCO_EMS_BRIDGE") String loopGuardSource,
        @DefaultValue("5") int deliveryAttemptsBeforeDlq) {

    public boolean isTopic() {
        return "topic".equalsIgnoreCase(destinationType);
    }
}
