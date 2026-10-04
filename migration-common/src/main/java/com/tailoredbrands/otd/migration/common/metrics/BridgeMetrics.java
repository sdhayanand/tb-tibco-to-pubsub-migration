package com.tailoredbrands.otd.migration.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Micrometer instruments shared by both bridges (exported on {@code /actuator/prometheus} and,
 * on GKE, scraped into Cloud Monitoring):
 *
 * <pre>
 *  bridge_messages_bridged_total{bridge}          successfully forwarded
 *  bridge_messages_duplicates_total{bridge}       redeliveries seen (JMSRedelivered / delivery_attempt &gt; 1)
 *  bridge_messages_failures_total{bridge,reason}  forward failed (will be retried by the source)
 *  bridge_messages_dlq_total{bridge}              poison messages sent to events-dlq
 *  bridge_messages_skipped_total{bridge}          loop-guard skips
 *  bridge_publish_seconds{bridge}                 forward latency (publish / send + ack)
 *  bridge_lag_seconds{bridge}                     age of the last forwarded message when forwarded
 *  bridge_last_message_age_seconds{bridge}        seconds since the last forwarded message
 *  bridge_running{bridge}                         1 when consuming, 0 when paused by phase
 * </pre>
 */
public class BridgeMetrics {

    private final Counter bridged;
    private final Counter duplicates;
    private final Counter dlq;
    private final Counter skipped;
    private final Timer publishTimer;
    private final MeterRegistry registry;
    private final Tags tags;
    private final AtomicLong lagMillis = new AtomicLong(0);
    private final AtomicReference<Instant> lastMessageAt = new AtomicReference<>(null);
    private final AtomicLong running = new AtomicLong(0);

    public BridgeMetrics(MeterRegistry registry, String bridgeName) {
        this.registry = registry;
        this.tags = Tags.of("bridge", bridgeName);
        this.bridged = Counter.builder("bridge.messages.bridged").tags(tags)
                .description("Messages forwarded successfully").register(registry);
        this.duplicates = Counter.builder("bridge.messages.duplicates").tags(tags)
                .description("Redelivered messages seen (possible duplicates)").register(registry);
        this.dlq = Counter.builder("bridge.messages.dlq").tags(tags)
                .description("Poison messages routed to events-dlq").register(registry);
        this.skipped = Counter.builder("bridge.messages.skipped").tags(tags)
                .description("Messages skipped by the loop guard").register(registry);
        this.publishTimer = Timer.builder("bridge.publish").tags(tags)
                .description("Forward latency").publishPercentiles(0.5, 0.95, 0.99).register(registry);
        Gauge.builder("bridge.lag.seconds", lagMillis, v -> v.get() / 1000.0).tags(tags)
                .description("Age of the last forwarded message at forward time").register(registry);
        Gauge.builder("bridge.last.message.age.seconds", this, m -> m.lastMessageAgeSeconds()).tags(tags)
                .description("Seconds since the last forwarded message (-1 = none yet)").register(registry);
        Gauge.builder("bridge.running", running, AtomicLong::get).tags(tags)
                .description("1 when the bridge is consuming").register(registry);
    }

    public void recordBridged(Instant sourceTimestamp) {
        bridged.increment();
        Instant now = Instant.now();
        lastMessageAt.set(now);
        if (sourceTimestamp != null) {
            lagMillis.set(Math.max(0, Duration.between(sourceTimestamp, now).toMillis()));
        }
    }

    public void recordDuplicate() {
        duplicates.increment();
    }

    public void recordFailure(String reason) {
        Counter.builder("bridge.messages.failures").tags(tags).tag("reason", reason == null ? "unknown" : reason)
                .description("Forward failures (retried by the source)").register(registry).increment();
    }

    public void recordDlq() {
        dlq.increment();
    }

    public void recordSkipped() {
        skipped.increment();
    }

    public void setRunning(boolean isRunning) {
        running.set(isRunning ? 1 : 0);
    }

    public Timer publishTimer() {
        return publishTimer;
    }

    public Instant lastMessageAt() {
        return lastMessageAt.get();
    }

    public double lastMessageAgeSeconds() {
        Instant last = lastMessageAt.get();
        return last == null ? -1 : Duration.between(last, Instant.now()).toMillis() / 1000.0;
    }

    public long lagMillis() {
        return lagMillis.get();
    }
}
