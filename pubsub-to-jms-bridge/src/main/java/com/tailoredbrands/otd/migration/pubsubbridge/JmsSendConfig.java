package com.tailoredbrands.otd.migration.pubsubbridge;

import com.tailoredbrands.otd.migration.common.jms.JmsConnectionProbe;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.DeliveryMode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

/**
 * Producer-side JMS wiring: the provider factory from {@code migration-common} is wrapped in a
 * {@link CachingConnectionFactory} (one connection, cached sessions/producers — the right pattern
 * for a {@link JmsTemplate}, which otherwise opens a connection per send) and the template is
 * session-transacted so a send is committed before the Pub/Sub ack.
 */
@Configuration(proxyBeanMethods = false)
public class JmsSendConfig {

    public static final String BRIDGE_NAME = "pubsub-to-jms";

    @Bean
    public BridgeMetrics bridgeMetrics(MeterRegistry registry) {
        return new BridgeMetrics(registry, BRIDGE_NAME);
    }

    @Bean(destroyMethod = "destroy")
    public CachingConnectionFactory cachingConnectionFactory(
            @Qualifier("jmsConnectionFactory") ConnectionFactory providerFactory) {
        CachingConnectionFactory caching = new CachingConnectionFactory(providerFactory);
        caching.setSessionCacheSize(4);
        caching.setCacheProducers(true);
        caching.setReconnectOnException(true);
        return caching;
    }

    @Bean
    public JmsTemplate legacyJmsTemplate(CachingConnectionFactory cachingConnectionFactory, BridgeProperties props) {
        JmsTemplate template = new JmsTemplate(cachingConnectionFactory);
        template.setSessionTransacted(true);
        template.setPubSubDomain(props.isTopic());
        template.setExplicitQosEnabled(true);
        template.setDeliveryMode(DeliveryMode.PERSISTENT);
        return template;
    }

    @Bean
    public JmsConnectionProbe jmsConnectionProbe(@Qualifier("jmsConnectionFactory") ConnectionFactory providerFactory) {
        return new JmsConnectionProbe(providerFactory);
    }
}
