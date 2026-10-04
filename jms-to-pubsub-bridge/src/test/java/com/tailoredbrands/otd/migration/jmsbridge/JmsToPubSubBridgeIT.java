package com.tailoredbrands.otd.migration.jmsbridge;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubSettings;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.MessageProducer;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end: legacy XML on Artemis queue {@code TB.ORDERS.OUT} → bridge → Pub/Sub emulator
 * topic {@code orders-v1}; asserts payload, attributes and ordering key. A poison message must
 * land on {@code events-dlq}. Requires Docker (runs in the Maven {@code verify} phase).
 */
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class JmsToPubSubBridgeIT {

    static final String PROJECT = "tb-it";
    static final String TOPIC = "orders-v1";
    static final String DLQ_TOPIC = "events-dlq";
    static final String TEST_SUBSCRIPTION = "orders-it";
    static final String DLQ_SUBSCRIPTION = "events-dlq-it";
    static final String QUEUE = "TB.ORDERS.OUT";

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
    static SubscriberStub pullStub;

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
        registry.add("bridge.source", () -> QUEUE);
        registry.add("bridge.target-topic", () -> TOPIC);
        registry.add("bridge.dlq-topic", () -> DLQ_TOPIC);
        registry.add("bridge.publish-timeout", () -> "20s");
        registry.add("migration.phase", () -> "SHADOW");
        registry.add("migration.control-subscription", () -> "");
    }

    @BeforeAll
    static void createTopology() throws Exception {
        clients = new PubSubClients(new PubSubSettings(PROJECT, PUBSUB.getEmulatorEndpoint()));
        try (TopicAdminClient topics = clients.topicAdminClient();
             SubscriptionAdminClient subs = clients.subscriptionAdminClient()) {
            topics.createTopic(TopicName.of(PROJECT, TOPIC));
            topics.createTopic(TopicName.of(PROJECT, DLQ_TOPIC));
            subs.createSubscription(Subscription.newBuilder()
                    .setName(ProjectSubscriptionName.of(PROJECT, TEST_SUBSCRIPTION).toString())
                    .setTopic(TopicName.of(PROJECT, TOPIC).toString())
                    .setEnableMessageOrdering(true)
                    .setAckDeadlineSeconds(10)
                    .build());
            subs.createSubscription(Subscription.newBuilder()
                    .setName(ProjectSubscriptionName.of(PROJECT, DLQ_SUBSCRIPTION).toString())
                    .setTopic(TopicName.of(PROJECT, DLQ_TOPIC).toString())
                    .setAckDeadlineSeconds(10)
                    .build());
        }
        pullStub = clients.subscriberStub();
    }

    @AfterAll
    static void shutdown() {
        if (pullStub != null) {
            pullStub.close();
        }
        if (clients != null) {
            clients.close();
        }
    }

    @Test
    void bridgesLegacyXmlToOrdersTopicWithAttributesAndOrderingKey() throws Exception {
        String xml = """
                <Order>
                  <OrderNbr>ORD-IT-0001</OrderNbr><OrderType>T</OrderType><StoreNbr>0412</StoreNbr>
                  <CustNbr>C-77812</CustNbr><OrderDate>2026-10-03T22:14:00Z</OrderDate>
                  <PromiseDate>2026-10-10</PromiseDate><Currency>USD</Currency><TotalAmt>649.99</TotalAmt>
                  <Lines>
                    <Line><LineNbr>1</LineNbr><SKU>MW-SUIT-NAVY-42R</SKU><Qty>1</Qty><UnitPrice>599.99</UnitPrice><FulfillType>P</FulfillType></Line>
                    <Line><LineNbr>2</LineNbr><SKU>ALT-HEM-TROUSER</SKU><Qty>1</Qty><UnitPrice>50.00</UnitPrice><FulfillType>A</FulfillType>
                      <Alteration><AltType>HEM</AltType><Measure>31.5</Measure><TailorShop>TS-EASTBAY</TailorShop></Alteration></Line>
                  </Lines>
                </Order>
                """;
        String jmsMessageId = sendToQueue(xml, "store-0412-txn-889213", Map.of("storeId", "0412", "bwProcess", "OrderOut"));

        List<ReceivedMessage> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    received.addAll(pull(TEST_SUBSCRIPTION));
                    return !received.isEmpty();
                });

        PubsubMessage msg = received.get(0).getMessage();
        assertThat(msg.getOrderingKey()).isEqualTo("0412");
        Map<String, String> attrs = msg.getAttributesMap();
        assertThat(attrs)
                .containsEntry("source", "TIBCO_EMS_BRIDGE")
                .containsEntry("eventType", "ORDER_CREATED")
                .containsEntry("schemaVersion", "1")
                .containsEntry("storeId", "0412")
                .containsEntry("correlationId", "store-0412-txn-889213")
                .containsEntry("legacyMessageId", jmsMessageId)
                .containsEntry("bwProcess", "OrderOut")
                .containsKey("eventId")
                .containsKey("legacyTimestamp");

        JsonNode body = JsonMappers.canonical().readTree(msg.getData().toStringUtf8());
        assertThat(body.get("eventType").asText()).isEqualTo("ORDER_CREATED");
        assertThat(body.get("source").asText()).isEqualTo("TIBCO_EMS_BRIDGE");
        assertThat(body.get("legacyMessageId").asText()).isEqualTo(jmsMessageId);
        assertThat(body.get("order").get("orderId").asText()).isEqualTo("ORD-IT-0001");
        assertThat(body.get("order").get("orderType").asText()).isEqualTo("TAILORED");
        assertThat(body.get("order").get("storeId").asText()).isEqualTo("0412");
        assertThat(body.get("order").get("totalAmount").decimalValue()).isEqualByComparingTo("649.99");
        assertThat(body.get("order").get("lines").size()).isEqualTo(2);
        assertThat(body.get("order").get("lines").get(1).get("fulfillmentType").asText()).isEqualTo("ALTERATION");

        // actuator reflects a running bridge in phase SHADOW
        String state = rest.getForObject("/actuator/bridge", String.class);
        assertThat(state).contains("\"bridge.state\":\"RUNNING\"").contains("\"phase\":\"SHADOW\"");
        String health = rest.getForObject("/actuator/health", String.class);
        assertThat(health).contains("\"status\":\"UP\"").contains("\"state\":\"RUNNING\"");
    }

    @Test
    void poisonMessageGoesToDlqAndIsConsumed() throws Exception {
        sendToQueue("<Order><OrderNbr>BAD-1</OrderNbr><OrderType>Q</OrderType></Order>", null, Map.of());

        List<ReceivedMessage> received = new ArrayList<>();
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    received.addAll(pull(DLQ_SUBSCRIPTION));
                    return !received.isEmpty();
                });

        PubsubMessage msg = received.get(0).getMessage();
        assertThat(msg.getAttributesMap()).containsKey("dlqReason")
                .containsEntry("dlqStage", "jms-to-pubsub-bridge")
                .containsEntry("originalTopic", TOPIC);
        assertThat(msg.getAttributesMap().get("dlqReason")).contains("OrderType");
        assertThat(msg.getData().toStringUtf8()).contains("BAD-1");
    }

    // ------------------------------------------------------------------ helpers

    private static String sendToQueue(String xml, String correlationId, Map<String, String> props) throws Exception {
        ConnectionFactory cf = new ActiveMQConnectionFactory(
                "tcp://" + ARTEMIS.getHost() + ":" + ARTEMIS.getMappedPort(61616));
        try (Connection connection = cf.createConnection()) {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(QUEUE));
            TextMessage message = session.createTextMessage(xml);
            if (correlationId != null) {
                message.setJMSCorrelationID(correlationId);
            }
            for (Map.Entry<String, String> e : props.entrySet()) {
                message.setStringProperty(e.getKey(), e.getValue());
            }
            producer.send(message);
            return message.getJMSMessageID();
        }
    }

    private static List<ReceivedMessage> pull(String subscription) {
        PullResponse response = pullStub.pullCallable().call(PullRequest.newBuilder()
                .setSubscription(ProjectSubscriptionName.of(PROJECT, subscription).toString())
                .setMaxMessages(10)
                .build());
        List<ReceivedMessage> messages = response.getReceivedMessagesList();
        if (!messages.isEmpty()) {
            List<String> ackIds = new ArrayList<>();
            for (ReceivedMessage m : messages) {
                ackIds.add(m.getAckId());
            }
            pullStub.acknowledgeCallable().call(AcknowledgeRequest.newBuilder()
                    .setSubscription(ProjectSubscriptionName.of(PROJECT, subscription).toString())
                    .addAllAckIds(ackIds)
                    .build());
        }
        return messages;
    }
}
