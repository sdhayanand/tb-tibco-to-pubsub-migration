package com.tailoredbrands.otd.migration.jmsbridge;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Thin wrapper over two {@link Publisher}s: the ordered {@code orders-v1} publisher and the
 * unordered {@code events-dlq} publisher. {@link #publishOrdered} blocks until Pub/Sub has
 * acknowledged the message, which is what makes the JMS commit (and thus the at-least-once
 * guarantee) safe.
 */
@Component
public class OrdersPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OrdersPublisher.class);

    private final Publisher ordersPublisher;
    private final Publisher dlqPublisher;
    private final Duration publishTimeout;
    private final String targetTopic;
    private final String dlqTopic;
    /** A poison payload is truncated for the DLQ so the DLQ publish itself cannot exceed the 10 MB limit. */
    static final int MAX_DLQ_PAYLOAD_CHARS = 1_000_000;

    public OrdersPublisher(PubSubClients clients, BridgeProperties props) throws IOException {
        this.ordersPublisher = clients.publisher(props.targetTopic(), true);
        this.dlqPublisher = clients.publisher(props.dlqTopic(), false);
        this.publishTimeout = props.publishTimeout();
        this.targetTopic = props.targetTopic();
        this.dlqTopic = props.dlqTopic();
        log.info("Publishing to {} (ordered) and {} (dlq) in project {}", targetTopic, dlqTopic, clients.project());
    }

    /**
     * Publishes with an ordering key and waits for the server-assigned message id.
     *
     * @throws PublishException when Pub/Sub did not accept the message within the timeout; the
     *                          ordering key is resumed so the next attempt is not rejected
     */
    public String publishOrdered(String json, Map<String, String> attributes, String orderingKey) {
        PubsubMessage.Builder builder = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(json))
                .putAllAttributes(attributes);
        if (orderingKey != null && !orderingKey.isBlank()) {
            builder.setOrderingKey(orderingKey);
        }
        ApiFuture<String> future = ordersPublisher.publish(builder.build());
        try {
            return future.get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            resume(orderingKey);
            throw new PublishException("Publish to " + targetTopic + " failed: " + e.getCause(), e.getCause());
        } catch (TimeoutException e) {
            resume(orderingKey);
            throw new PublishException("Publish to " + targetTopic + " timed out after " + publishTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            resume(orderingKey);
            throw new PublishException("Interrupted while publishing to " + targetTopic, e);
        }
    }

    /** Sends a poison message to {@code events-dlq} with the DLQ attributes of ARCHITECTURE.md section 3.4. */
    public String publishDlq(String originalPayload, Map<String, String> originalAttributes, String reason,
                             String stage) {
        Map<String, String> attrs = new LinkedHashMap<>();
        if (originalAttributes != null) {
            attrs.putAll(originalAttributes);
        }
        attrs.put("dlqReason", reason == null ? "unknown" : reason.length() > 900 ? reason.substring(0, 900) : reason);
        attrs.put("dlqStage", stage);
        attrs.put("originalTopic", targetTopic);
        String payload = originalPayload == null ? "" : originalPayload;
        if (payload.length() > MAX_DLQ_PAYLOAD_CHARS) {
            attrs.put("dlqTruncated", "true");
            attrs.put("dlqOriginalLength", Integer.toString(payload.length()));
            payload = payload.substring(0, MAX_DLQ_PAYLOAD_CHARS);
        }
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(payload))
                .putAllAttributes(attrs)
                .build();
        try {
            return dlqPublisher.publish(message).get(publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new PublishException("Publish to " + dlqTopic + " failed: " + e.getCause(), e.getCause());
        } catch (TimeoutException e) {
            throw new PublishException("Publish to " + dlqTopic + " timed out after " + publishTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PublishException("Interrupted while publishing to " + dlqTopic, e);
        }
    }

    private void resume(String orderingKey) {
        if (orderingKey != null && !orderingKey.isBlank()) {
            try {
                ordersPublisher.resumePublish(orderingKey);
            } catch (RuntimeException e) {
                log.warn("resumePublish({}) failed: {}", orderingKey, e.toString());
            }
        }
    }

    @Override
    public void close() {
        PubSubClients.shutdownQuietly(ordersPublisher);
        PubSubClients.shutdownQuietly(dlqPublisher);
    }

    /** Unchecked so a plain {@link jakarta.jms.MessageListener} can propagate it to trigger rollback. */
    public static class PublishException extends RuntimeException {
        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
