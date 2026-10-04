package com.tailoredbrands.otd.migration.pubsubbridge;

import com.tailoredbrands.otd.migration.common.headers.HeaderMapper;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import jakarta.jms.TextMessage;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Sends one legacy XML TextMessage to {@code JMS_DESTINATION} in a local JMS transaction
 * (the {@link JmsTemplate} is session-transacted, so the send is committed before this method
 * returns; the caller acks the Pub/Sub message only after that).
 *
 * <p>JMS properties: {@code storeId}, {@code eventType}, {@code eventId}, {@code source},
 * {@code schemaVersion}, {@code pubsubMessageId} (+ {@code legacyMessageId} if the event was
 * itself bridged); {@code JMSCorrelationID} = the event's correlationId.</p>
 */
@Component
public class LegacyJmsSender {

    private final JmsTemplate jmsTemplate;
    private final BridgeProperties props;

    public LegacyJmsSender(JmsTemplate legacyJmsTemplate, BridgeProperties props) {
        this.jmsTemplate = legacyJmsTemplate;
        this.props = props;
    }

    public void send(String xml, OrderEvent event, Map<String, String> attributes, String pubsubMessageId) {
        jmsTemplate.send(props.destination(), session -> {
            TextMessage message = session.createTextMessage(xml);
            HeaderMapper.applyToMessage(message, attributes, pubsubMessageId);
            // make sure the well-known properties are present even if the publisher forgot the attributes
            if (!message.propertyExists(HeaderMapper.PROP_STORE_ID) && event.order().storeId() != null) {
                message.setStringProperty(HeaderMapper.PROP_STORE_ID, event.order().storeId());
            }
            if (!message.propertyExists(HeaderMapper.PROP_EVENT_TYPE) && event.eventType() != null) {
                message.setStringProperty(HeaderMapper.PROP_EVENT_TYPE, event.eventType());
            }
            if (!message.propertyExists(HeaderMapper.ATTR_EVENT_ID) && event.eventId() != null) {
                message.setStringProperty(HeaderMapper.ATTR_EVENT_ID, event.eventId());
            }
            if (message.getJMSCorrelationID() == null && event.correlationId() != null) {
                message.setJMSCorrelationID(event.correlationId());
            }
            message.setStringProperty("orderId", event.order().orderId());
            return message;
        });
    }
}
