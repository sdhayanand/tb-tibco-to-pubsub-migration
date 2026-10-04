package com.tailoredbrands.otd.migration.pubsubbridge;

import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhase;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhaseSource;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/** {@code GET /actuator/bridge}, {@code POST /actuator/bridge {"phase":"DUAL_RUN"}} (this instance only). */
@Component
@Endpoint(id = "bridge")
public class BridgeEndpoint {

    private final SubscriberStateController controller;
    private final MigrationPhaseSource phaseSource;
    private final BridgeMetrics metrics;
    private final BridgeProperties props;

    public BridgeEndpoint(SubscriberStateController controller, MigrationPhaseSource phaseSource,
                          BridgeMetrics metrics, BridgeProperties props) {
        this.controller = controller;
        this.phaseSource = phaseSource;
        this.metrics = metrics;
        this.props = props;
    }

    @ReadOperation
    public Map<String, Object> state() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("bridge.state", controller.state().name());
        out.put("phase", phaseSource.current().name());
        out.put("phaseOrigin", phaseSource.lastOrigin());
        out.put("phaseChangedAt", phaseSource.lastChangedAt().toString());
        out.put("sourceSubscription", props.sourceSubscription());
        out.put("destination", props.destination());
        out.put("lastMessageAt", metrics.lastMessageAt() == null ? null : metrics.lastMessageAt().toString());
        out.put("lastMessageAgeSeconds", metrics.lastMessageAgeSeconds());
        out.put("lagMillis", metrics.lagMillis());
        return out;
    }

    @WriteOperation
    public Map<String, Object> setPhase(String phase) {
        phaseSource.update(MigrationPhase.parse(phase), "actuator");
        return state();
    }
}
