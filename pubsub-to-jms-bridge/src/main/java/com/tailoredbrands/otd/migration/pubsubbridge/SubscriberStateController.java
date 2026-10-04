package com.tailoredbrands.otd.migration.pubsubbridge;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.Subscriber;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhase;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhaseSource;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Runs the streaming-pull {@link Subscriber} on {@code SOURCE_SUBSCRIPTION} only in phases that
 * bridge Pub/Sub → legacy (DUAL_RUN, PUBSUB_PRIMARY). A {@link Subscriber} cannot be restarted
 * once stopped, so every start creates a fresh instance.
 */
@Component
public class SubscriberStateController implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SubscriberStateController.class);

    public enum State { RUNNING, PAUSED }

    private final PubSubClients clients;
    private final CanonicalEventReceiver receiver;
    private final MigrationPhaseSource phaseSource;
    private final BridgeMetrics metrics;
    private final BridgeProperties props;
    private volatile Subscriber subscriber;
    private volatile String lastFailure;
    private volatile boolean lifecycleRunning;

    public SubscriberStateController(PubSubClients clients, CanonicalEventReceiver receiver,
                                     MigrationPhaseSource phaseSource, BridgeMetrics metrics, BridgeProperties props) {
        this.clients = clients;
        this.receiver = receiver;
        this.phaseSource = phaseSource;
        this.metrics = metrics;
        this.props = props;
    }

    @Override
    public void start() {
        lifecycleRunning = true;
        phaseSource.onChange(this::apply);
    }

    @Override
    public void stop() {
        lifecycleRunning = false;
        stopSubscriber();
    }

    @Override
    public boolean isRunning() {
        return lifecycleRunning;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 200;
    }

    public synchronized void apply(MigrationPhase phase) {
        boolean shouldRun = phase.bridgesPubSubToLegacy();
        if (shouldRun && subscriber == null) {
            log.warn("Phase {} -> starting subscriber on {} (bridge RUNNING)", phase, props.sourceSubscription());
            startSubscriber();
        } else if (!shouldRun && subscriber != null) {
            log.warn("Phase {} -> stopping subscriber (bridge PAUSED); outstanding messages are nacked", phase);
            stopSubscriber();
        } else {
            log.info("Phase {} -> bridge stays {}", phase, state());
        }
    }

    private void startSubscriber() {
        Subscriber s = clients.subscriber(props.sourceSubscription(), receiver, props.flowControlMessages());
        s.addListener(new ApiService.Listener() {
            @Override
            public void failed(ApiService.State from, Throwable failure) {
                lastFailure = failure.toString();
                metrics.recordFailure("subscriber");
                metrics.setRunning(false);
                log.error("Subscriber on {} failed from state {}", props.sourceSubscription(), from, failure);
            }
        }, Runnable::run);
        s.startAsync().awaitRunning();
        subscriber = s;
        lastFailure = null;
        metrics.setRunning(true);
    }

    private synchronized void stopSubscriber() {
        Subscriber s = subscriber;
        subscriber = null;
        metrics.setRunning(false);
        if (s == null) {
            return;
        }
        try {
            s.stopAsync().awaitTerminated(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Subscriber did not stop cleanly: {}", e.toString());
        }
    }

    public State state() {
        Subscriber s = subscriber;
        return s != null && s.isRunning() ? State.RUNNING : State.PAUSED;
    }

    public boolean subscriberHealthy() {
        Subscriber s = subscriber;
        return s == null || s.isRunning();
    }

    public String lastFailure() {
        return lastFailure;
    }

    public MigrationPhase phase() {
        return phaseSource.current();
    }
}
