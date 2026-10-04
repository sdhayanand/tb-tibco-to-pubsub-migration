package com.tailoredbrands.otd.migration.jmsbridge;

import com.tailoredbrands.otd.migration.common.jms.JmsConnectionProbe;
import com.tailoredbrands.otd.migration.common.metrics.BridgeMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Wires the Spring {@link DefaultMessageListenerContainer} that consumes the legacy destination.
 *
 * <ul>
 *   <li>Transacted session (local JMS transaction): the message is only committed after the
 *       listener returns, i.e. after Pub/Sub acknowledged the publish. This is the JMS
 *       equivalent of CLIENT_ACKNOWLEDGE-after-publish, with rollback on failure.</li>
 *   <li>Queue by default; a topic with a durable subscription when
 *       {@code JMS_SOURCE_TYPE=topic} (needs {@code JMS_CLIENT_ID} and {@code JMS_DURABLE_NAME}).</li>
 *   <li>{@code autoStartup=false}: {@link BridgeStateController} starts / stops it according to
 *       the migration phase.</li>
 *   <li>Concurrency 1 by default so a queue's FIFO order maps 1:1 onto Pub/Sub ordering keys;
 *       raising {@code BRIDGE_CONCURRENCY} trades ordering for throughput.</li>
 * </ul>
 */
@Configuration(proxyBeanMethods = false)
public class BridgeListenerConfig {

    private static final Logger log = LoggerFactory.getLogger(BridgeListenerConfig.class);

    public static final String BRIDGE_NAME = "jms-to-pubsub";

    @Bean
    public BridgeMetrics bridgeMetrics(MeterRegistry registry) {
        return new BridgeMetrics(registry, BRIDGE_NAME);
    }

    @Bean
    public JmsConnectionProbe jmsConnectionProbe(ConnectionFactory connectionFactory) {
        return new JmsConnectionProbe(connectionFactory);
    }

    @Bean
    public DefaultMessageListenerContainer legacyListenerContainer(ConnectionFactory connectionFactory,
                                                                   LegacyOrderMessageListener listener,
                                                                   BridgeProperties props,
                                                                   BridgeMetrics metrics) {
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setDestinationName(props.source());
        container.setMessageListener(listener);
        container.setSessionTransacted(true);
        container.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        container.setConcurrentConsumers(Math.max(1, props.concurrency()));
        container.setMaxConcurrentConsumers(Math.max(1, props.concurrency()));
        container.setReceiveTimeout(props.receiveTimeout().toMillis());
        container.setAutoStartup(false);
        container.setBeanName("legacyListenerContainer");
        // reconnect every 5 s forever if the broker goes away (EMS fault-tolerant failover takes a few seconds)
        container.setBackOff(new FixedBackOff(5000L, FixedBackOff.UNLIMITED_ATTEMPTS));
        if (props.hasSelector()) {
            container.setMessageSelector(props.selector());
        }
        if (props.isTopic()) {
            container.setPubSubDomain(true);
            container.setSubscriptionDurable(true);
            if (props.clientId() == null || props.clientId().isBlank()
                    || props.durableName() == null || props.durableName().isBlank()) {
                throw new IllegalStateException("JMS_SOURCE_TYPE=topic requires JMS_CLIENT_ID and JMS_DURABLE_NAME");
            }
            container.setClientId(props.clientId());
            container.setSubscriptionName(props.durableName());
        }
        container.setErrorHandler(t -> {
            metrics.recordFailure("listener");
            log.error("Listener error (message will be redelivered by the broker): {}", t.toString());
        });
        container.setExceptionListener(e -> {
            metrics.recordFailure("connection");
            log.error("JMS connection exception: {}", e.toString());
        });
        log.info("Legacy listener on {} '{}' (concurrency={}, selector={})",
                props.isTopic() ? "topic" : "queue", props.source(), props.concurrency(),
                props.hasSelector() ? props.selector() : "none");
        return container;
    }
}
