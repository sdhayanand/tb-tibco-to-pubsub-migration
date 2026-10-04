package com.tailoredbrands.otd.migration.pubsubbridge;

import com.tailoredbrands.otd.migration.common.jms.JmsConnectionProbe;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * {@code GET /actuator/health} component {@code bridge}: JMS connectivity (cached probe),
 * subscriber state, age of the last forwarded message. DOWN when the bridge should be running
 * but the JMS side is unreachable or the subscriber died; UP with {@code state=PAUSED} otherwise.
 */
@Component("bridge")
public class BridgeHealthIndicator implements HealthIndicator {

    private final SubscriberStateController controller;
    private final JmsConnectionProbe probe;
    private final BridgeMetrics metrics;

    public BridgeHealthIndicator(SubscriberStateController controller, JmsConnectionProbe probe, BridgeMetrics metrics) {
        this.controller = controller;
        this.probe = probe;
        this.metrics = metrics;
    }

    @Override
    public Health health() {
        JmsConnectionProbe.Result jms = probe.check();
        boolean paused = controller.state() == SubscriberStateController.State.PAUSED;
        boolean shouldRun = controller.phase().bridgesPubSubToLegacy();
        boolean healthy = paused && !shouldRun || (jms.up() && controller.subscriberHealthy());
        Instant lastMessageAt = metrics.lastMessageAt();
        Health.Builder builder = healthy ? Health.up() : Health.down();
        return builder
                .withDetail("state", controller.state().name())
                .withDetail("phase", controller.phase().name())
                .withDetail("jmsConnection", jms.up() ? "UP" : "DOWN")
                .withDetail("jmsProvider", jms.detail() == null ? "" : jms.detail())
                .withDetail("subscriberFailure", controller.lastFailure() == null ? "" : controller.lastFailure())
                .withDetail("lastMessageAt", lastMessageAt == null ? "never" : lastMessageAt.toString())
                .withDetail("lastMessageAgeSeconds", metrics.lastMessageAgeSeconds())
                .withDetail("lagMillis", metrics.lagMillis())
                .build();
    }
}
