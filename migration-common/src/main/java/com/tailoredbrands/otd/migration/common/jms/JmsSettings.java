package com.tailoredbrands.otd.migration.common.jms;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.util.Locale;

/**
 * {@code jms.*} properties, fed from the environment in each application.yml:
 *
 * <pre>
 *  JMS_PROVIDER = artemis | ibmmq | ems      (default artemis)
 *  JMS_URL      = tcp://host:61616           (artemis)  |  tcp://ems-host:7222 (ems)
 *  JMS_USER / JMS_PASSWORD
 *  MQ_QMGR MQ_CHANNEL MQ_HOST MQ_PORT        (ibmmq only)
 * </pre>
 */
@ConfigurationProperties(prefix = "jms")
public record JmsSettings(
        @DefaultValue("artemis") String provider,
        @DefaultValue("tcp://localhost:61616") String url,
        String user,
        String password,
        @DefaultValue Mq mq) {

    public enum Provider {
        ARTEMIS, IBMMQ, EMS;

        public static Provider parse(String value) {
            if (value == null || value.isBlank()) {
                return ARTEMIS;
            }
            String v = value.trim().toUpperCase(Locale.ROOT).replace("-", "").replace("_", "");
            return switch (v) {
                case "ARTEMIS", "ACTIVEMQ", "AMQ" -> ARTEMIS;
                case "IBMMQ", "MQ", "WMQ", "WEBSPHEREMQ" -> IBMMQ;
                case "EMS", "TIBCO", "TIBCOEMS", "TIBJMS" -> EMS;
                default -> throw new IllegalArgumentException("Unknown JMS_PROVIDER '" + value
                        + "' (expected artemis | ibmmq | ems)");
            };
        }
    }

    /** IBM MQ client connection details (ignored for other providers). */
    public record Mq(
            @DefaultValue("QM1") String qmgr,
            @DefaultValue("DEV.APP.SVRCONN") String channel,
            @DefaultValue("localhost") String host,
            @DefaultValue("1414") int port,
            @DefaultValue("tb-migration-bridge") String appName) {
    }

    public Provider providerType() {
        return Provider.parse(provider);
    }

    public boolean hasCredentials() {
        return user != null && !user.isBlank();
    }

    /** Builds settings from plain environment variables (for non-Spring callers such as tests). */
    public static JmsSettings fromEnvironment() {
        String port = env("MQ_PORT", "1414");
        return new JmsSettings(
                env("JMS_PROVIDER", "artemis"),
                env("JMS_URL", "tcp://localhost:61616"),
                System.getenv("JMS_USER"),
                System.getenv("JMS_PASSWORD"),
                new Mq(env("MQ_QMGR", "QM1"), env("MQ_CHANNEL", "DEV.APP.SVRCONN"), env("MQ_HOST", "localhost"),
                        Integer.parseInt(port), env("MQ_APP_NAME", "tb-migration-bridge")));
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    @Override
    public String toString() {
        return "JmsSettings{provider=" + provider + ", url=" + url + ", user=" + user + ", password="
                + (password == null ? "null" : "***") + ", mq=" + mq + "}";
    }
}
