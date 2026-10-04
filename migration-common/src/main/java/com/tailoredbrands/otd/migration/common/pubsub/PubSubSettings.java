package com.tailoredbrands.otd.migration.common.pubsub;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code pubsub.*} properties. {@code PUBSUB_PROJECT} and {@code PUBSUB_EMULATOR_HOST} map onto
 * them in each application.yml (and via relaxed binding of {@code PUBSUB_PROJECT} directly).
 *
 * @param project      GCP project id (any string for the emulator)
 * @param emulatorHost host:port of the Pub/Sub emulator; empty/null means real Pub/Sub
 */
@ConfigurationProperties(prefix = "pubsub")
public record PubSubSettings(
        @DefaultValue("tb-local") String project,
        String emulatorHost) {

    public boolean usesEmulator() {
        return emulatorHost != null && !emulatorHost.isBlank();
    }

    public static PubSubSettings fromEnvironment() {
        String project = System.getenv("PUBSUB_PROJECT");
        return new PubSubSettings(project == null || project.isBlank() ? "tb-local" : project,
                System.getenv("PUBSUB_EMULATOR_HOST"));
    }
}
