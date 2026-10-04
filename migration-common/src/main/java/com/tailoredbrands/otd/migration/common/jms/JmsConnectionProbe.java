package com.tailoredbrands.otd.migration.common.jms;

import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;

import java.time.Duration;
import java.time.Instant;

/**
 * Cheap, cached JMS connectivity check for health indicators: opens and closes a connection at
 * most once per {@code ttl} (default 30 s) so Kubernetes probes do not hammer the broker.
 */
public class JmsConnectionProbe {

    public record Result(boolean up, String detail, Instant checkedAt) {
    }

    private final ConnectionFactory connectionFactory;
    private final Duration ttl;
    private volatile Result last;

    public JmsConnectionProbe(ConnectionFactory connectionFactory) {
        this(connectionFactory, Duration.ofSeconds(30));
    }

    public JmsConnectionProbe(ConnectionFactory connectionFactory, Duration ttl) {
        this.connectionFactory = connectionFactory;
        this.ttl = ttl;
    }

    public Result check() {
        Result cached = last;
        Instant now = Instant.now();
        if (cached != null && cached.checkedAt().plus(ttl).isAfter(now)) {
            return cached;
        }
        synchronized (this) {
            cached = last;
            if (cached != null && cached.checkedAt().plus(ttl).isAfter(now)) {
                return cached;
            }
            Result fresh = probe(now);
            last = fresh;
            return fresh;
        }
    }

    private Result probe(Instant now) {
        Connection connection = null;
        try {
            connection = connectionFactory.createConnection();
            String meta = connection.getMetaData() == null ? "connected"
                    : connection.getMetaData().getJMSProviderName() + " " + connection.getMetaData().getProviderVersion();
            return new Result(true, meta, now);
        } catch (JMSException | RuntimeException e) {
            return new Result(false, e.getClass().getSimpleName() + ": " + e.getMessage(), now);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException ignored) {
                    // nothing to do
                }
            }
        }
    }
}
