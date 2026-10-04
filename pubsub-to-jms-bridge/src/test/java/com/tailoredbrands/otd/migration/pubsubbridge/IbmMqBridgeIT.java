package com.tailoredbrands.otd.migration.pubsubbridge;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubClients;
import com.tailoredbrands.otd.migration.common.pubsub.PubSubSettings;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Same flow as {@link PubSubToJmsBridgeIT} but with {@code JMS_PROVIDER=ibmmq} against the IBM MQ
 * developer image ({@code icr.io/ibm-messaging/mq:latest}, queue manager QM1, channel
 * DEV.APP.SVRCONN, user app, queue DEV.QUEUE.1). The image is ~1 GB and needs the license
 * acceptance, so the test is opt-in: {@code RUN_MQ_IT=true}. CI runs it in the
 * {@code mq-integration} job on {@code main}:
 * <pre>
 *   RUN_MQ_IT=true mvn -B -ntp -pl pubsub-to-jms-bridge -am verify -Dit.test=IbmMqBridgeIT \
 *       -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Testcontainers
@EnabledIfEnvironmentVariable(named = "RUN_MQ_IT", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IbmMqBridgeIT {

    static final String PROJECT = "tb-it";
    static final String TOPIC = "orders-v1";
    static final String DLQ_TOPIC = "events-dlq";
    static final String SUBSCRIPTION = "orders-to-legacy-mq";
    static final String QUEUE = "DEV.QUEUE.1";
    static final String MQ_USER = "app";
    static final String MQ_PASSWORD = "passw0rd";

    @Container
    static final GenericContainer<?> MQ = new GenericContainer<>(DockerImageName.parse("icr.io/ibm-messaging/mq:latest"))
            .withEnv("LICENSE", "accept")
            .withEnv("MQ_QMGR_NAME", "QM1")
            .withEnv("MQ_APP_PASSWORD", MQ_PASSWORD)
            .withExposedPorts(1414)
            // AMQ5806I: "Queued Publish/Subscribe Daemon started" is logged once the queue manager accepts connections
            .waitingFor(Wait.forLogMessage("(?s).*AMQ5806I.*", 1))
            .withStartupTimeout(Duration.ofMinutes(5));

    @Container
    static final PubSubEmulatorContainer PUBSUB = new PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"));

    static PubSubClients clients;
    static Publisher publisher;
    static final LegacyXmlMapper MAPPER = new LegacyXmlMapper();

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("jms.provider", () -> "ibmmq");
        registry.add("jms.user", () -> MQ_USER);
        registry.add("jms.password", () -> MQ_PASSWORD);
        registry.add("jms.mq.qmgr", () -> "QM1");
        registry.add("jms.mq.channel", () -> "DEV.APP.SVRCONN");
        registry.add("jms.mq.host", MQ::getHost);
        registry.add("jms.mq.port", () -> MQ.getMappedPort(1414));
        registry.add("pubsub.project", () -> PROJECT);
        registry.add("pubsub.emulator-host", PUBSUB::getEmulatorEndpoint);
        registry.add("bridge.source-subscription", () -> SUBSCRIPTION);
        registry.add("bridge.destination", () -> QUEUE);
        registry.add("bridge.dlq-topic", () -> DLQ_TOPIC);
        registry.add("migration.phase", () -> "PUBSUB_PRIMARY");
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
    void forwardsCanonicalEventToIbmMqQueue() throws Exception {
        OrderEvent event = PubSubToJmsBridgeIT.sampleEvent("ORD-MQ-0001", OrderEvent.SOURCE_ORDER_INTAKE_API);
        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(MAPPER.toJson(event)))
                .putAllAttributes(Map.of("eventType", event.eventType(), "schemaVersion", "1",
                        "source", event.source(), "storeId", "0412", "correlationId", event.correlationId(),
                        "eventId", event.eventId()))
                .setOrderingKey("0412")
                .build();
        String pubsubId = publisher.publish(message).get(30, TimeUnit.SECONDS);

        Message received = receiveFromMq(Duration.ofSeconds(90));
        assertThat(received).as("message on " + QUEUE).isNotNull().isInstanceOf(TextMessage.class);
        String xml = ((TextMessage) received).getText();
        assertThat(xml).contains("<OrderNbr>ORD-MQ-0001</OrderNbr>");
        assertThat(MAPPER.parseOrder(xml)).isEqualTo(event.order());
        assertThat(received.getJMSCorrelationID()).isEqualTo(event.correlationId());
        assertThat(received.getStringProperty("storeId")).isEqualTo("0412");
        assertThat(received.getStringProperty("pubsubMessageId")).isEqualTo(pubsubId);
    }

    static Message receiveFromMq(Duration timeout) throws Exception {
        MQConnectionFactory cf = new MQConnectionFactory();
        cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        cf.setHostName(MQ.getHost());
        cf.setPort(MQ.getMappedPort(1414));
        cf.setQueueManager("QM1");
        cf.setChannel("DEV.APP.SVRCONN");
        cf.setStringProperty(WMQConstants.USERID, MQ_USER);
        cf.setStringProperty(WMQConstants.PASSWORD, MQ_PASSWORD);
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        try (Connection connection = cf.createConnection(MQ_USER, MQ_PASSWORD)) {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue(QUEUE));
            return consumer.receive(timeout.toMillis());
        }
    }
}
