package com.tailoredbrands.otd.migration.jmsbridge;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * {@code bridge.*} properties (see application.yml for the env var mapping).
 *
 * @param source              JMS_SOURCE, queue or topic name (TB.ORDERS.OUT)
 * @param sourceType          JMS_SOURCE_TYPE queue | topic
 * @param clientId            JMS_CLIENT_ID, required for a durable topic subscription
 * @param durableName         JMS_DURABLE_NAME, durable subscription name
 * @param selector            optional JMS message selector (e.g. {@code storeId IN ('0412','0088')})
 * @param concurrency         BRIDGE_CONCURRENCY, consumers; keep 1 to preserve per-queue order
 * @param targetTopic         TARGET_TOPIC (orders-v1)
 * @param dlqTopic            DLQ_TOPIC (events-dlq)
 * @param defaultEventType    used when the JMS message has no eventType property
 * @param maxDeliveryAttempts when JMSXDeliveryCount exceeds this, a failing message goes to the DLQ
 * @param publishTimeout      how long to wait for the Pub/Sub publish future before rolling back
 * @param receiveTimeout      DMLC receive timeout (how fast stop() takes effect)
 */
@ConfigurationProperties(prefix = "bridge")
public record BridgeProperties(
        @DefaultValue("TB.ORDERS.OUT") String source,
        @DefaultValue("queue") String sourceType,
        String clientId,
        String durableName,
        String selector,
        @DefaultValue("1") int concurrency,
        @DefaultValue("orders-v1") String targetTopic,
        @DefaultValue("events-dlq") String dlqTopic,
        @DefaultValue("ORDER_CREATED") String defaultEventType,
        @DefaultValue("5") int maxDeliveryAttempts,
        @DefaultValue("30s") Duration publishTimeout,
        @DefaultValue("1s") Duration receiveTimeout) {

    public boolean isTopic() {
        return "topic".equalsIgnoreCase(sourceType);
    }

    public boolean hasSelector() {
        return selector != null && !selector.isBlank();
    }
}
