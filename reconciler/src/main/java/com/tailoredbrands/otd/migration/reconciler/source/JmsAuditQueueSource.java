package com.tailoredbrands.otd.migration.reconciler.source;

import com.tailoredbrands.otd.migration.common.model.Order;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlException;
import com.tailoredbrands.otd.migration.common.xml.LegacyXmlMapper;
import com.tailoredbrands.otd.migration.reconciler.model.SideRecord;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * Legacy side via a non-destructive {@link QueueBrowser} over the audit queue
 * ({@code TB.ORDERS.AUDIT}: TIBCO BW writes a copy of every outbound order there, with a TTL
 * of a few days). Browsing is the one JMS feature with no Pub/Sub equivalent (see
 * CONCEPT-MAPPING.md) — on the Pub/Sub side the same role is played by the BigQuery
 * subscription / Dataflow tables.
 *
 * <p>Messages are filtered by JMSTimestamp with a selector ({@code JMSTimestamp >= from AND
 * JMSTimestamp < to}) so a large audit queue is not transferred in full; EMS and Artemis support
 * JMSTimestamp in selectors, IBM MQ does too.</p>
 */
public class JmsAuditQueueSource implements RecordSource {

    private static final Logger log = LoggerFactory.getLogger(JmsAuditQueueSource.class);

    private final ConnectionFactory connectionFactory;
    private final String queueName;
    private final LegacyXmlMapper xmlMapper = new LegacyXmlMapper();

    public JmsAuditQueueSource(ConnectionFactory connectionFactory, String queueName) {
        this.connectionFactory = connectionFactory;
        this.queueName = queueName;
    }

    @Override
    public String describe() {
        return "JMS browse " + queueName;
    }

    @Override
    public List<SideRecord> read(Instant from, Instant to) throws JMSException {
        List<SideRecord> out = new ArrayList<>();
        String selector = "JMSTimestamp >= " + from.toEpochMilli() + " AND JMSTimestamp < " + to.toEpochMilli();
        Connection connection = connectionFactory.createConnection();
        try {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            QueueBrowser browser = session.createBrowser(queue, selector);
            int seen = 0;
            int skipped = 0;
            Enumeration<?> messages = browser.getEnumeration();
            while (messages != null && messages.hasMoreElements()) {
                Object o = messages.nextElement();
                if (!(o instanceof Message message)) {
                    continue;
                }
                seen++;
                try {
                    SideRecord record = toRecord(message);
                    if (record.inWindow(from, to)) {
                        out.add(record);
                    }
                } catch (LegacyXmlException | JMSException e) {
                    skipped++;
                    log.warn("{}: skipping message {}: {}", describe(), safeId(message), e.getMessage());
                }
            }
            browser.close();
            log.info("{}: browsed {} messages, {} usable, {} skipped (selector: {})", describe(), seen, out.size(),
                    skipped, selector);
            return out;
        } finally {
            connection.close();
        }
    }

    private SideRecord toRecord(Message message) throws JMSException {
        String xml;
        if (message instanceof TextMessage text) {
            xml = text.getText();
        } else if (message instanceof BytesMessage bytes) {
            byte[] buf = new byte[(int) bytes.getBodyLength()];
            bytes.readBytes(buf);
            xml = new String(buf, StandardCharsets.UTF_8);
        } else {
            throw new LegacyXmlException("Unsupported message type " + message.getClass().getSimpleName());
        }
        Order order = xmlMapper.parseOrder(xml);
        Instant ts = message.getJMSTimestamp() > 0 ? Instant.ofEpochMilli(message.getJMSTimestamp()) : order.orderedAt();
        return new SideRecord(order.orderId(), order.totalAmount(), order.lineCount(), ts, message.getJMSMessageID());
    }

    private static String safeId(Message message) {
        try {
            return message.getJMSMessageID();
        } catch (JMSException e) {
            return "?";
        }
    }
}
