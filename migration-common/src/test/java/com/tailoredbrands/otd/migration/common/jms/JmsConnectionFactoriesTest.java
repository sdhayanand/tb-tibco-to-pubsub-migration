package com.tailoredbrands.otd.migration.common.jms;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import jakarta.jms.ConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Only builds factories; no broker connection is opened. */
class JmsConnectionFactoriesTest {

    @Test
    void buildsArtemisFactory() {
        JmsSettings settings = new JmsSettings("artemis", "tcp://localhost:61616", "admin", "admin", null);
        ConnectionFactory cf = JmsConnectionFactories.create(settings);
        assertThat(cf).isInstanceOf(ActiveMQConnectionFactory.class);
        assertThat(((ActiveMQConnectionFactory) cf).getUser()).isEqualTo("admin");
    }

    @Test
    void buildsIbmMqFactoryInClientMode() throws Exception {
        JmsSettings settings = new JmsSettings("ibmmq", null, "app", "passw0rd",
                new JmsSettings.Mq("QM1", "DEV.APP.SVRCONN", "mq.example", 1414, "tb-test"));
        ConnectionFactory cf = JmsConnectionFactories.create(settings);
        assertThat(cf).isInstanceOf(MQConnectionFactory.class);
        MQConnectionFactory mq = (MQConnectionFactory) cf;
        assertThat(mq.getQueueManager()).isEqualTo("QM1");
        assertThat(mq.getChannel()).isEqualTo("DEV.APP.SVRCONN");
        assertThat(mq.getHostName()).isEqualTo("mq.example");
        assertThat(mq.getPort()).isEqualTo(1414);
    }

    @Test
    void emsWithoutClientJarFailsWithActionableMessage() {
        org.junit.jupiter.api.Assumptions.assumeTrue(!emsClientOnClasspath(), "tibjms.jar present (-Pems); skipping");
        JmsSettings settings = new JmsSettings("ems", "tcp://ems:7222", "bridge", "secret", null);
        assertThatThrownBy(() -> JmsConnectionFactories.create(settings))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("-Pems");
    }

    private static boolean emsClientOnClasspath() {
        try {
            Class.forName(JmsConnectionFactories.EMS_CONNECTION_FACTORY_CLASS);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Test
    void toStringMasksPassword() {
        JmsSettings settings = new JmsSettings("artemis", "tcp://x", "u", "topsecret", null);
        assertThat(settings.toString()).doesNotContain("topsecret").contains("***");
    }
}
