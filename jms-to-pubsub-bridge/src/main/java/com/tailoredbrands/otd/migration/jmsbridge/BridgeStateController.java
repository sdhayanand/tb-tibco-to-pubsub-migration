package com.tailoredbrands.otd.migration.jmsbridge;

import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhase;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhaseSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Starts the listener container in phases that bridge legacy → Pub/Sub (SHADOW, DUAL_RUN,
 * PUBSUB_PRIMARY) and stops it otherwise (LEGACY_ONLY, CUTOVER). Reacts to runtime phase
 * changes from the {@code migration-control} topic or the actuator endpoint.
 */
@Component
public class BridgeStateController implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(BridgeStateController.class);

    public enum State { RUNNING, PAUSED }

    private final DefaultMessageListenerContainer container;
    private final MigrationPhaseSource phaseSource;
    private final BridgeMetrics metrics;
    private volatile boolean lifecycleRunning;

    public BridgeStateController(DefaultMessageListenerContainer legacyListenerContainer,
                                 MigrationPhaseSource phaseSource, BridgeMetrics metrics) {
        this.container = legacyListenerContainer;
        this.phaseSource = phaseSource;
        this.metrics = metrics;
    }

    @Override
    public void start() {
        lifecycleRunning = true;
        phaseSource.onChange(this::apply);
    }

    @Override
    public void stop() {
        lifecycleRunning = false;
        if (container.isRunning()) {
            log.info("Shutting down: stopping legacy listener container");
            container.stop();
        }
        metrics.setRunning(false);
    }

    @Override
    public boolean isRunning() {
        return lifecycleRunning;
    }

    @Override
    public int getPhase() {
        // after the container bean (default phase 0 for SmartLifecycle containers… DMLC uses Integer.MAX_VALUE),
        // but before the migration-control subscriber so a queued phase change finds us ready
        return Integer.MAX_VALUE - 200;
    }

    public synchronized void apply(MigrationPhase phase) {
        boolean shouldRun = phase.bridgesLegacyToPubSub();
        if (shouldRun && !container.isRunning()) {
            log.warn("Phase {} -> starting legacy listener (bridge RUNNING)", phase);
            container.start();
        } else if (!shouldRun && container.isRunning()) {
            log.warn("Phase {} -> stopping legacy listener (bridge PAUSED); in-flight message finishes first", phase);
            container.stop();
        } else {
            log.info("Phase {} -> bridge stays {}", phase, state());
        }
        metrics.setRunning(container.isRunning());
    }

    public State state() {
        return container.isRunning() ? State.RUNNING : State.PAUSED;
    }

    public MigrationPhase phase() {
        return phaseSource.current();
    }

    public boolean containerActive() {
        return container.isActive();
    }

    public int activeConsumers() {
        return container.getActiveConsumerCount();
    }
}
