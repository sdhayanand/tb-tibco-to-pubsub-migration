package com.tailoredbrands.otd.migration.common.phase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Holds the current {@link MigrationPhase}. Seeded from the {@code MIGRATION_PHASE} environment
 * variable and optionally updated at runtime by {@link MigrationControlSubscriber}
 * (Pub/Sub topic {@code migration-control}) or the actuator endpoint. Listeners are notified
 * on every change, on the caller's thread; the bridges use that to start / stop consuming.
 */
public class MigrationPhaseSource {

    private static final Logger log = LoggerFactory.getLogger(MigrationPhaseSource.class);

    private final AtomicReference<MigrationPhase> current;
    private final List<Consumer<MigrationPhase>> listeners = new CopyOnWriteArrayList<>();
    private volatile String lastOrigin;
    private volatile Instant lastChangedAt;

    public MigrationPhaseSource(MigrationPhase initial) {
        this.current = new AtomicReference<>(initial == null ? MigrationPhase.LEGACY_ONLY : initial);
        this.lastOrigin = "env";
        this.lastChangedAt = Instant.now();
    }

    public MigrationPhase current() {
        return current.get();
    }

    public String lastOrigin() {
        return lastOrigin;
    }

    public Instant lastChangedAt() {
        return lastChangedAt;
    }

    /**
     * Switches to {@code next}. Returns true when the phase actually changed (listeners were called).
     *
     * @param origin "env", "pubsub:migration-control", "actuator" ... for the audit log
     */
    public boolean update(MigrationPhase next, String origin) {
        if (next == null) {
            return false;
        }
        MigrationPhase previous = current.getAndSet(next);
        if (previous == next) {
            log.info("Migration phase unchanged: {} (origin={})", next, origin);
            return false;
        }
        lastOrigin = origin;
        lastChangedAt = Instant.now();
        log.warn("MIGRATION PHASE CHANGE {} -> {} (origin={})", previous, next, origin);
        for (Consumer<MigrationPhase> l : listeners) {
            try {
                l.accept(next);
            } catch (RuntimeException e) {
                log.error("Phase listener failed for {}", next, e);
            }
        }
        return true;
    }

    /** Registers a listener; it is called immediately with the current phase so state converges. */
    public void onChange(Consumer<MigrationPhase> listener) {
        listeners.add(listener);
        listener.accept(current.get());
    }
}
