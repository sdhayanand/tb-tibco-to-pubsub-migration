package com.tailoredbrands.otd.migration.jmsbridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * jms-to-pubsub-bridge: consumes legacy order XML from TIBCO EMS (Artemis locally, IBM MQ
 * optional) and publishes canonical {@code OrderEvent} JSON to Pub/Sub {@code orders-v1}.
 * See README.md and docs/RUNBOOK.md.
 */
// Boot's JMS auto-configuration is excluded: the ConnectionFactory comes from JmsProviderConfig
// (JMS_PROVIDER) and the bridge wires its own template / listener container.
@SpringBootApplication(
        scanBasePackages = "com.tailoredbrands.otd.migration",
        exclude = {JmsAutoConfiguration.class, ArtemisAutoConfiguration.class})
@EnableConfigurationProperties(BridgeProperties.class)
public class JmsToPubSubBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(JmsToPubSubBridgeApplication.class, args);
    }
}
