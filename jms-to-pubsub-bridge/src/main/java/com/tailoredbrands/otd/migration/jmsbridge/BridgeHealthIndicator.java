package com.tailoredbrands.otd.migration.jmsbridge;

import com.tailoredbrands.otd.migration.common.jms.JmsConnectionProbe;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * {@code GET /actuator/health} component {@code bridge}:
 * <pre>
 * "bridge": { "status": "UP", "details": { "state": "RUNNING", "phase": "SHADOW",
 *             "jmsConnection": "UP", "jmsProvider": "ActiveMQ Artemis 2.x",
 *             "lastMessageAt": "...", "lastMessageAgeSeconds": 12.3, "activeConsumers": 1 } }
 * </pre>
 * DOWN when the JMS connection cannot be established while the bridge should be running.
 * A paused bridge (phase LEGACY_ONLY / CUTOVER) is UP with {@code state=PAUSED}, so pods are
 * not restarted just because the migration has not started yet.
 */
@Component("bridge")
public class BridgeHealthIndicator implements HealthIndicator {

    private final BridgeStateController controller;
    private final JmsConnectionProbe probe;
    private final BridgeMetrics metrics;

    public BridgeHealthIndicator(BridgeStateController controller, JmsConnectionProbe probe, BridgeMetrics metrics) {
        this.controller = controller;
        this.probe = probe;
        this.metrics = metrics;
    }

    @Override
    public Health health() {
        JmsConnectionProbe.Result jms = probe.check();
        Instant lastMessageAt = metrics.lastMessageAt();
        Health.Builder builder = (jms.up() || controller.state() == BridgeStateController.State.PAUSED)
                ? Health.up() : Health.down();
        return builder
                .withDetail("state", controller.state().name())
                .withDetail("phase", controller.phase().name())
                .withDetail("jmsConnection", jms.up() ? "UP" : "DOWN")
                .withDetail("jmsProvider", jms.detail() == null ? "" : jms.detail())
                .withDetail("lastMessageAt", lastMessageAt == null ? "never" : lastMessageAt.toString())
                .withDetail("lastMessageAgeSeconds", metrics.lastMessageAgeSeconds())
                .withDetail("lagMillis", metrics.lagMillis())
                .withDetail("activeConsumers", controller.activeConsumers())
                .build();
    }
}
