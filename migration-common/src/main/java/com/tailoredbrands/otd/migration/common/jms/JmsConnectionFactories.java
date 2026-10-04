package com.tailoredbrands.otd.migration.common.jms;

import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Builds a {@link jakarta.jms.ConnectionFactory} for the selected provider. Everything above
 * this class only sees the Jakarta JMS API, which is what makes TIBCO EMS a drop-in:
 *
 * <ul>
 *   <li><b>artemis</b> – ActiveMQ Artemis ({@code artemis-jakarta-client}), the dev/test stand-in
 *       for EMS (same JMS semantics: queues, topics, durable subscribers, selectors, DLQ).</li>
 *   <li><b>ibmmq</b> – IBM MQ ({@code com.ibm.mq.jakarta.client}) in client mode
 *       ({@code WMQ_CM_CLIENT}).</li>
 *   <li><b>ems</b> – TIBCO EMS via reflection on {@code com.tibco.tibjms.TibjmsConnectionFactory}.
 *       The EMS jar is not on Maven Central, so it is only required on the runtime classpath when
 *       {@code JMS_PROVIDER=ems} (build with {@code -Pems} after installing {@code lib/tibjms.jar}
 *       as {@code com.tibco:tibjms:10.3.0}).</li>
 * </ul>
 */
public final class JmsConnectionFactories {

    private static final Logger log = LoggerFactory.getLogger(JmsConnectionFactories.class);

    public static final String EMS_CONNECTION_FACTORY_CLASS = "com.tibco.tibjms.TibjmsConnectionFactory";

    private JmsConnectionFactories() {
    }

    public static ConnectionFactory create(JmsSettings settings) {
        JmsSettings.Provider provider = settings.providerType();
        log.info("Creating JMS ConnectionFactory for provider {} ({})", provider, settings);
        return switch (provider) {
            case ARTEMIS -> artemis(settings);
            case IBMMQ -> ibmMq(settings);
            case EMS -> ems(settings);
        };
    }

    static ConnectionFactory artemis(JmsSettings s) {
        if (s.hasCredentials()) {
            return new ActiveMQConnectionFactory(s.url(), s.user(), s.password());
        }
        return new ActiveMQConnectionFactory(s.url());
    }

    static ConnectionFactory ibmMq(JmsSettings s) {
        JmsSettings.Mq mq = s.mq() == null ? new JmsSettings.Mq("QM1", "DEV.APP.SVRCONN", "localhost", 1414,
                "tb-migration-bridge") : s.mq();
        try {
            MQConnectionFactory cf = new MQConnectionFactory();
            cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
            cf.setHostName(mq.host());
            cf.setPort(mq.port());
            cf.setQueueManager(mq.qmgr());
            cf.setChannel(mq.channel());
            cf.setStringProperty(WMQConstants.WMQ_APPLICATIONNAME, mq.appName());
            if (s.hasCredentials()) {
                cf.setStringProperty(WMQConstants.USERID, s.user());
                cf.setStringProperty(WMQConstants.PASSWORD, s.password() == null ? "" : s.password());
                cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
            }
            return cf;
        } catch (JMSException e) {
            throw new IllegalStateException("Cannot configure IBM MQ connection factory: " + e.getMessage(), e);
        }
    }

    /**
     * Reflection-based so the EMS jar is only needed when this provider is selected.
     * Equivalent to:
     * <pre>
     *   TibjmsConnectionFactory cf = new TibjmsConnectionFactory(url);
     *   cf.setUserName(user); cf.setUserPassword(password);
     * </pre>
     */
    static ConnectionFactory ems(JmsSettings s) {
        try {
            Class<?> cls = Class.forName(EMS_CONNECTION_FACTORY_CLASS);
            Constructor<?> ctor = cls.getConstructor(String.class);
            Object cf = ctor.newInstance(s.url());
            if (s.hasCredentials()) {
                Method setUser = cls.getMethod("setUserName", String.class);
                setUser.invoke(cf, s.user());
                Method setPassword = cls.getMethod("setUserPassword", String.class);
                setPassword.invoke(cf, s.password() == null ? "" : s.password());
            }
            // Fault-tolerant pair reconnects: 10 attempts, 1 s apart (ignored if the method is absent).
            invokeIfPresent(cls, cf, "setReconnAttemptCount", 10);
            invokeIfPresent(cls, cf, "setReconnAttemptDelay", 1000);
            if (!(cf instanceof ConnectionFactory factory)) {
                throw new IllegalStateException(EMS_CONNECTION_FACTORY_CLASS + " does not implement jakarta.jms.ConnectionFactory; "
                        + "make sure the Jakarta build of tibjms.jar (EMS 10.x) is on the classpath");
            }
            return factory;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JMS_PROVIDER=ems but " + EMS_CONNECTION_FACTORY_CLASS
                    + " is not on the classpath. Build with -Pems after installing lib/tibjms.jar "
                    + "(see README).", e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException("Unexpected TIBCO EMS client API: " + e.getMessage(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("TIBCO EMS connection factory failed: " + e.getTargetException(), e.getTargetException());
        }
    }

    private static void invokeIfPresent(Class<?> cls, Object target, String method, int arg) {
        try {
            cls.getMethod(method, int.class).invoke(target, arg);
        } catch (NoSuchMethodException e) {
            log.debug("EMS client has no {}(int); skipping", method);
        } catch (IllegalAccessException | InvocationTargetException e) {
            log.warn("EMS {}({}) failed: {}", method, arg, e.toString());
        }
    }
}
