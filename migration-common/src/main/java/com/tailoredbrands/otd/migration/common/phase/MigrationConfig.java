package com.tailoredbrands.otd.migration.common.phase;

import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@code migration.phase} (env {@code MIGRATION_PHASE}) seeds the phase;
 * {@code migration.control-subscription} (env {@code MIGRATION_CONTROL_SUBSCRIPTION}) optionally
 * enables live updates from the {@code migration-control} topic.
 */
@Configuration(proxyBeanMethods = false)
public class MigrationConfig {

    @Bean
    public MigrationPhaseSource migrationPhaseSource(
            @Value("${migration.phase:${MIGRATION_PHASE:LEGACY_ONLY}}") String phase) {
        return new MigrationPhaseSource(MigrationPhase.parse(phase));
    }

    @Bean
    public MigrationControlSubscriber migrationControlSubscriber(
            PubSubClients clients,
            MigrationPhaseSource phaseSource,
            @Value("${migration.control-subscription:${MIGRATION_CONTROL_SUBSCRIPTION:}}") String subscription) {
        return new MigrationControlSubscriber(clients, subscription, phaseSource);
    }
}
