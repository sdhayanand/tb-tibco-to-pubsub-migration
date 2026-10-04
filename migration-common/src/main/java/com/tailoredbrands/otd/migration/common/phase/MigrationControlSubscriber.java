package com.tailoredbrands.otd.migration.common.phase;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.util.concurrent.TimeUnit;

/**
 * Listens on the {@code migration-control} topic (ARCHITECTURE.md section 4) and updates the
 * {@link MigrationPhaseSource}. Message format, either of:
 * <ul>
 *   <li>attribute {@code phase=DUAL_RUN}</li>
 *   <li>data {@code DUAL_RUN} (plain text)</li>
 *   <li>data {@code {"phase":"DUAL_RUN"}}</li>
 * </ul>
 * Any message that cannot be parsed is logged and acked (never nacked, to avoid a poison
 * message blocking later phase changes).
 *
 * <p>Pub/Sub delivers each message to <em>one</em> subscriber of a subscription, so every
 * bridge deployment should get its own subscription on the topic (for example
 * {@code migration-control-jms-to-pubsub}); set {@code MIGRATION_CONTROL_SUBSCRIPTION}
 * accordingly. An empty value disables this subscriber and {@code MIGRATION_PHASE} alone drives
 * the bridge.</p>
 */
public class MigrationControlSubscriber implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MigrationControlSubscriber.class);

    private final PubSubClients clients;
    private final String subscriptionId;
    private final MigrationPhaseSource phaseSource;
    private volatile Subscriber subscriber;
    private volatile boolean running;

    public MigrationControlSubscriber(PubSubClients clients, String subscriptionId, MigrationPhaseSource phaseSource) {
        this.clients = clients;
        this.subscriptionId = subscriptionId;
        this.phaseSource = phaseSource;
    }

    public boolean isEnabled() {
        return subscriptionId != null && !subscriptionId.isBlank();
    }

    @Override
    public void start() {
        if (!isEnabled()) {
            log.info("migration-control subscriber disabled (MIGRATION_CONTROL_SUBSCRIPTION empty); phase comes from env only");
            return;
        }
        Subscriber s = clients.subscriber(subscriptionId, this::onMessage, 10);
        s.addListener(new ApiService.Listener() {
            @Override
            public void failed(ApiService.State from, Throwable failure) {
                log.error("migration-control subscriber failed (state {}); phase changes via Pub/Sub are unavailable "
                        + "until restart, MIGRATION_PHASE env still applies", from, failure);
            }
        }, Runnable::run);
        s.startAsync().awaitRunning();
        this.subscriber = s;
        this.running = true;
        log.info("Listening for phase changes on subscription {}", subscriptionId);
    }

    private void onMessage(PubsubMessage message, AckReplyConsumer consumer) {
        try {
            String raw = message.getAttributesOrDefault("phase", null);
            if (raw == null || raw.isBlank()) {
                String data = message.getData().toStringUtf8().trim();
                if (data.startsWith("{")) {
                    JsonNode node = JsonMappers.canonical().readTree(data);
                    JsonNode phaseNode = node.get("phase");
                    raw = phaseNode == null ? null : phaseNode.asText();
                } else {
                    raw = data;
                }
            }
            MigrationPhase next = MigrationPhase.parse(raw);
            phaseSource.update(next, "pubsub:" + subscriptionId + ":" + message.getMessageId());
        } catch (Exception e) {
            log.error("Ignoring unparseable migration-control message {}: {}", message.getMessageId(), e.toString());
        } finally {
            consumer.ack();
        }
    }

    @Override
    public void stop() {
        Subscriber s = subscriber;
        if (s != null) {
            try {
                s.stopAsync().awaitTerminated(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.warn("migration-control subscriber did not stop cleanly: {}", e.toString());
            }
        }
        subscriber = null;
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // start late (after the bridge containers exist), stop early
        return Integer.MAX_VALUE - 100;
    }
}
