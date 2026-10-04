package com.tailoredbrands.otd.migration.common.pubsub;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Registers the {@link PubSubClients} factory (closed on context shutdown). */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PubSubSettings.class)
public class PubSubConfig {

    @Bean(destroyMethod = "close")
    public PubSubClients pubSubClients(PubSubSettings settings) {
        return new PubSubClients(settings);
    }
}
