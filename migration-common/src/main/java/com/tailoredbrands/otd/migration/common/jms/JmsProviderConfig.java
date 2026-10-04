package com.tailoredbrands.otd.migration.common.jms;

import jakarta.jms.ConnectionFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Exposes the provider-specific {@link ConnectionFactory} selected by {@code JMS_PROVIDER}
 * as a Spring bean. Spring Boot's own JMS/Artemis auto-configuration is not used (the bridges
 * do not depend on {@code spring-boot-starter-artemis}), so this is the only factory.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(JmsSettings.class)
public class JmsProviderConfig {

    @Bean
    public ConnectionFactory jmsConnectionFactory(JmsSettings settings) {
        return JmsConnectionFactories.create(settings);
    }
}
