package com.tailoredbrands.otd.migration.pubsubbridge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.JmsAutoConfiguration;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * pubsub-to-jms-bridge: consumes canonical {@code OrderEvent}s from Pub/Sub subscription
 * {@code orders-to-legacy-mq} and sends legacy order XML to IBM MQ {@code ERP.ORDERS.IN}
 * (Artemis locally, EMS optional) so the ERP keeps working until it is migrated.
 */
// Boot's JMS auto-configuration is excluded: the ConnectionFactory comes from JmsProviderConfig
// (JMS_PROVIDER) and the bridge wires its own template / listener container.
@SpringBootApplication(
        scanBasePackages = "com.tailoredbrands.otd.migration",
        exclude = {JmsAutoConfiguration.class, ArtemisAutoConfiguration.class})
@EnableConfigurationProperties(BridgeProperties.class)
public class PubSubToJmsBridgeApplication {

    public static void main(String[] args) {
        SpringApplication.run(PubSubToJmsBridgeApplication.class, args);
    }
}
