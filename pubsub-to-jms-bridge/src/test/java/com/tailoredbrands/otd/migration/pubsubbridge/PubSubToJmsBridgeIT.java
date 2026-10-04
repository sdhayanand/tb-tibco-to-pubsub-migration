package com.tailoredbrands.otd.migration.pubsubbridge;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import com.tailoredbrands.otd.migration.common.model.Order;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.model.OrderLine;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubSettings;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end: canonical {@code OrderEvent} published on the Pub/Sub emulator → bridge →
 * legacy XML on Artemis queue {@code ERP.ORDERS.IN} with JMS properties mapped back. Also
 * checks the loop guard (events from TIBCO_EMS_BRIDGE are swallowed). Requires Docker.
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PubSubToJmsBridgeIT {

    static final String PROJECT = "tb-it";
    static final String TOPIC = "orders-v1";
    static final String DLQ_TOPIC = "events-dlq";
    static final String SUBSCRIPTION = "orders-to-legacy-mq";
    static final String QUEUE = "ERP.ORDERS.IN";

    @Container
    static final GenericContainer<?> ARTEMIS = new GenericContainer<>(
            DockerImageName.parse("apache/activemq-artemis:latest-alpine"))
            .withEnv("ANONYMOUS_LOGIN", "true")
            .withExposedPorts(61616)
            .waitingFor(Wait.forLogMessage("(?s).*AMQ221007.*", 1))
            .withStartupTimeout(Duration.ofMinutes(3));

    @Container
    static final PubSubEmulatorContainer PUBSUB = new PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    static PubSubClients clients;
    static Publisher publisher;
    static final LegacyXmlMapper MAPPER = new LegacyXmlMapper();

    @Autowired
    TestRestTemplate rest;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("jms.provider", () -> "artemis");
        registry.add("jms.url", () -> "tcp://" + ARTEMIS.getHost() + ":" + ARTEMIS.getMappedPort(61616));
        registry.add("jms.user", () -> "");
        registry.add("jms.password", () -> "");
        registry.add("pubsub.project", () -> PROJECT);
        registry.add("pubsub.emulator-host", PUBSUB::getEmulatorEndpoint);
        registry.add("bridge.source-subscription", () -> SUBSCRIPTION);
        registry.add("bridge.destination", () -> QUEUE);
        registry.add("bridge.dlq-topic", () -> DLQ_TOPIC);
        registry.add("migration.phase", () -> "DUAL_RUN");
        registry.add("migration.control-subscription", () -> "");
    }

    @BeforeAll
    static void createTopology() throws Exception {
        clients = new PubSubClients(new PubSubSettings(PROJECT, PUBSUB.getEmulatorEndpoint()));
        try (TopicAdminClient topics = clients.topicAdminClient();
             SubscriptionAdminClient subs = clients.subscriptionAdminClient()) {
            topics.createTopic(TopicName.of(PROJECT, TOPIC));
            topics.createTopic(TopicName.of(PROJECT, DLQ_TOPIC));
            // NB: production adds filter attributes.source != "TIBCO_EMS_BRIDGE"; the bridge's own loop guard
            // is exercised here instead, so the test does not depend on emulator filter support.
            subs.createSubscription(Subscription.newBuilder()
                    .setName(ProjectSubscriptionName.of(PROJECT, SUBSCRIPTION).toString())
                    .setTopic(TopicName.of(PROJECT, TOPIC).toString())
                    .setEnableMessageOrdering(true)
                    .setAckDeadlineSeconds(10)
                    .build());
        }
        publisher = clients.publisher(TOPIC, true);
    }

    @AfterAll
    static void shutdown() {
        PubSubClients.shutdownQuietly(publisher);
        if (clients != null) {
            clients.close();
        }
    }

    @Test
    void forwardsCanonicalEventAsLegacyXmlWithJmsProperties() throws Exception {
        OrderEvent event = sampleEvent("ORD-IT-9001", OrderEvent.SOURCE_ORDER_INTAKE_API);
        String pubsubId = publish(event);

        Message received = receiveFromQueue(Duration.ofSeconds(60));
        assertThat(received).as("message on " + QUEUE).isNotNull().isInstanceOf(TextMessage.class);
        String xml = ((TextMessage) received).getText();
        assertThat(xml).contains("<OrderNbr>ORD-IT-9001</OrderNbr>")
                .contains("<OrderType>T</OrderType>")
                .contains("<StoreNbr>0412</StoreNbr>")
                .contains("<TotalAmt>649.99</TotalAmt>")
                .contains("<FulfillType>A</FulfillType>");
        assertThat(MAPPER.parseOrder(xml)).isEqualTo(event.order());

        assertThat(received.getJMSCorrelationID()).isEqualTo(event.correlationId());
        assertThat(received.getStringProperty("storeId")).isEqualTo("0412");
        assertThat(received.getStringProperty("eventType")).isEqualTo("ORDER_CREATED");
        assertThat(received.getStringProperty("eventId")).isEqualTo(event.eventId());
        assertThat(received.getStringProperty("source")).isEqualTo(OrderEvent.SOURCE_ORDER_INTAKE_API);
        assertThat(received.getStringProperty("pubsubMessageId")).isEqualTo(pubsubId);
        assertThat(received.getStringProperty("orderId")).isEqualTo("ORD-IT-9001");

        String state = rest.getForObject("/actuator/bridge", String.class);
        assertThat(state).contains("\"bridge.state\":\"RUNNING\"").contains("\"phase\":\"DUAL_RUN\"");
    }

    @Test
    void loopGuardSwallowsEventsBridgedFromEms() throws Exception {
        OrderEvent looped = sampleEvent("ORD-IT-LOOP", OrderEvent.SOURCE_TIBCO_EMS_BRIDGE);
        publish(looped);
        OrderEvent canary = sampleEvent("ORD-IT-CANARY", OrderEvent.SOURCE_ORDER_INTAKE_API);
        publish(canary);

        // the canary is the first (and only) message to reach the queue: the looped event was acked and dropped
        Message first = receiveFromQueue(Duration.ofSeconds(60));
        assertThat(first).isNotNull();
        assertThat(((TextMessage) first).getText()).contains("ORD-IT-CANARY").doesNotContain("ORD-IT-LOOP");
        Message second = receiveFromQueue(Duration.ofSeconds(3));
        assertThat(second).isNull();
    }

    // ------------------------------------------------------------------ helpers

    static OrderEvent sampleEvent(String orderId, String source) {
        Order order = new Order(orderId, "TAILORED", "STORE", "0412", "C-77812",
                Instant.parse("2026-10-03T22:14:00Z"), LocalDate.of(2026, 10, 10), "USD", new BigDecimal("649.99"),
                List.of(new OrderLine(1, "MW-SUIT-NAVY-42R", 1, new BigDecimal("599.99"), "STORE_PICKUP", null),
                        new OrderLine(2, "ALT-HEM-TROUSER", 1, new BigDecimal("50.00"), "ALTERATION", null)),
                null, null);
        return new OrderEvent(UUID.randomUUID().toString(), OrderEvent.TYPE_ORDER_CREATED,
                Instant.parse("2026-10-03T22:14:05.120Z"), OrderEvent.SCHEMA_VERSION, source,
                "store-0412-txn-" + orderId, null, order);
    }

    static String publish(OrderEvent event) throws Exception {
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(MAPPER.toJson(event)))
                .putAllAttributes(Map.of(
                        "eventType", event.eventType(),
                        "schemaVersion", event.schemaVersion(),
                        "source", event.source(),
                        "storeId", event.order().storeId(),
                        "correlationId", event.correlationId(),
                        "eventId", event.eventId()))
                .setOrderingKey(event.order().storeId())
                .build();
        return publisher.publish(message).get(30, TimeUnit.SECONDS);
    }

    static Message receiveFromQueue(Duration timeout) throws Exception {
        ConnectionFactory cf = new ActiveMQConnectionFactory(
                "tcp://" + ARTEMIS.getHost() + ":" + ARTEMIS.getMappedPort(61616));
        try (Connection connection = cf.createConnection()) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
            return consumer.receive(timeout.toMillis());
        }
    }
}
