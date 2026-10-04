package com.tailoredbrands.otd.migration.pubsubbridge;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Publishes poison events to {@code events-dlq} (attributes per ARCHITECTURE.md section 3.4). */
@Component
public class DlqPublisher implements AutoCloseable {

    private final Publisher publisher;
    private final String topic;

    public DlqPublisher(PubSubClients clients, BridgeProperties props) throws IOException {
        this.topic = props.dlqTopic();
        this.publisher = clients.publisher(topic, false);
    }

    public String publish(String payload, Map<String, String> originalAttributes, String reason, String stage,
                          String originalSubscription) {
        Map<String, String> attrs = new LinkedHashMap<>();
        if (originalAttributes != null) {
            attrs.putAll(originalAttributes);
        }
        attrs.remove("googclient_deliveryattempt");
        attrs.put("dlqReason", reason == null ? "unknown" : reason.length() > 900 ? reason.substring(0, 900) : reason);
        attrs.put("dlqStage", stage);
        attrs.put("originalTopic", "orders-v1");
        attrs.put("originalSubscription", originalSubscription);
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(payload == null ? "" : payload))
                .putAllAttributes(attrs)
                .build();
        try {
            return publisher.publish(message).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("Publish to " + topic + " failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        PubSubClients.shutdownQuietly(publisher);
    }
}
