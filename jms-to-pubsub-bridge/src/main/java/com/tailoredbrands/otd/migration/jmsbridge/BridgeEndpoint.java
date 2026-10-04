package com.tailoredbrands.otd.migration.jmsbridge;

import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhase;
import com.tailoredbrands.otd.migration.common.phase.MigrationPhaseSource;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * {@code GET /actuator/bridge} → {@code {"bridge.state":"PAUSED","phase":"LEGACY_ONLY",...}}.
 * {@code POST /actuator/bridge {"phase":"DUAL_RUN"}} → switches the phase for this instance only
 * (an operator escape hatch; the runbook uses {@code migration-control} / {@code kubectl set env}
 * so every replica agrees).
 */
@Component
@Endpoint(id = "bridge")
public class BridgeEndpoint {

    private final BridgeStateController controller;
    private final MigrationPhaseSource phaseSource;
    private final BridgeMetrics metrics;

    public BridgeEndpoint(BridgeStateController controller, MigrationPhaseSource phaseSource, BridgeMetrics metrics) {
        this.controller = controller;
        this.phaseSource = phaseSource;
        this.metrics = metrics;
    }

    @ReadOperation
    public Map<String, Object> state() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("bridge.state", controller.state().name());
        out.put("phase", phaseSource.current().name());
        out.put("phaseOrigin", phaseSource.lastOrigin());
        out.put("phaseChangedAt", phaseSource.lastChangedAt().toString());
        out.put("containerActive", controller.containerActive());
        out.put("activeConsumers", controller.activeConsumers());
        out.put("lastMessageAt", metrics.lastMessageAt() == null ? null : metrics.lastMessageAt().toString());
        out.put("lastMessageAgeSeconds", metrics.lastMessageAgeSeconds());
        out.put("lagMillis", metrics.lagMillis());
        return out;
    }

    @WriteOperation
    public Map<String, Object> setPhase(String phase) {
        MigrationPhase next = MigrationPhase.parse(phase);
        phaseSource.update(next, "actuator");
        return state();
    }
}
