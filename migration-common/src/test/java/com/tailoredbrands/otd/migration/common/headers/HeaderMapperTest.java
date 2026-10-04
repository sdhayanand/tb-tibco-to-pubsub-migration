package com.tailoredbrands.otd.migration.common.headers;

import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HeaderMapperTest {

    @Test
    void mapsJmsHeadersAndPropertiesToAttributes() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("storeId", "0412");
        props.put("eventType", "ORDER_CREATED");
        props.put("JMSXDeliveryCount", "3");
        props.put("JMS_IBM_Format", "MQSTR"); // provider noise, dropped
        props.put("bwProcess", "OrderOut");   // arbitrary custom property, kept

        Map<String, String> attrs = HeaderMapper.toAttributes("ID:EMS-SERVER.1A2B", "corr-1",
                1_790_000_000_000L, true, props);

        assertThat(attrs)
                .containsEntry("legacyMessageId", "ID:EMS-SERVER.1A2B")
                .containsEntry("correlationId", "corr-1")
                .containsEntry("legacyTimestamp", Instant.ofEpochMilli(1_790_000_000_000L).toString())
                .containsEntry("legacyDeliveryCount", "3")
                .containsEntry("legacyRedelivered", "true")
                .containsEntry("storeId", "0412")
                .containsEntry("eventType", "ORDER_CREATED")
                .containsEntry("bwProcess", "OrderOut")
                .doesNotContainKey("JMS_IBM_Format")
                .doesNotContainKey("JMSXDeliveryCount");
        assertThat(HeaderMapper.legacyTimestamp(attrs)).isEqualTo(Instant.ofEpochMilli(1_790_000_000_000L));
    }

    @Test
    void omitsAbsentHeaders() {
        Map<String, String> attrs = HeaderMapper.toAttributes(null, "", null, false, Map.of());
        assertThat(attrs).isEmpty();
        assertThat(HeaderMapper.legacyTimestamp(attrs)).isNull();
    }

    @Test
    void readsFromJmsMessage() throws JMSException {
        TextMessage message = mock(TextMessage.class);
        when(message.getJMSMessageID()).thenReturn("ID:1");
        when(message.getJMSCorrelationID()).thenReturn("corr");
        when(message.getJMSTimestamp()).thenReturn(1000L);
        when(message.getJMSRedelivered()).thenReturn(false);
        when(message.getPropertyNames()).thenReturn(Collections.enumeration(List.of("storeId", "eventType")));
        when(message.getObjectProperty("storeId")).thenReturn("0412");
        when(message.getObjectProperty("eventType")).thenReturn("ORDER_CANCELLED");

        Map<String, String> attrs = HeaderMapper.toAttributes(message);

        assertThat(attrs).containsEntry("legacyMessageId", "ID:1")
                .containsEntry("correlationId", "corr")
                .containsEntry("legacyTimestamp", "1970-01-01T00:00:01Z")
                .containsEntry("storeId", "0412")
                .containsEntry("eventType", "ORDER_CANCELLED")
                .doesNotContainKey("legacyRedelivered");
    }

    @Test
    void reverseMapsAttributesToJmsProperties() throws JMSException {
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("correlationId", "corr-9");
        attrs.put("storeId", "0412");
        attrs.put("eventType", "ORDER_CREATED");
        attrs.put("eventId", "evt-1");
        attrs.put("source", "ORDER_INTAKE_API");
        attrs.put("schemaVersion", "1");
        attrs.put("googclient_schemaencoding", "JSON"); // not in the allow-list
        attrs.put("weird-name", "x");

        Map<String, String> props = HeaderMapper.toJmsProperties(attrs, "pubsub-msg-123");
        assertThat(props).containsEntry("storeId", "0412")
                .containsEntry("eventType", "ORDER_CREATED")
                .containsEntry("eventId", "evt-1")
                .containsEntry("source", "ORDER_INTAKE_API")
                .containsEntry("schemaVersion", "1")
                .containsEntry("pubsubMessageId", "pubsub-msg-123")
                .doesNotContainKey("correlationId")
                .doesNotContainKey("googclient_schemaencoding")
                .doesNotContainKey("weird-name");

        TextMessage message = mock(TextMessage.class);
        HeaderMapper.applyToMessage(message, attrs, "pubsub-msg-123");
        verify(message).setJMSCorrelationID("corr-9");
        verify(message).setStringProperty("storeId", "0412");
        verify(message).setStringProperty("pubsubMessageId", "pubsub-msg-123");
        verify(message, never()).setStringProperty("weird-name", "x");
        verify(message, never()).setStringProperty("correlationId", "corr-9");
    }

    @Test
    void validatesJmsPropertyNames() {
        assertThat(HeaderMapper.isValidJmsPropertyName("storeId")).isTrue();
        assertThat(HeaderMapper.isValidJmsPropertyName("store-id")).isFalse();
        assertThat(HeaderMapper.isValidJmsPropertyName("1abc")).isFalse();
        assertThat(HeaderMapper.isValidJmsPropertyName("JMSXGroupID")).isFalse();
        assertThat(HeaderMapper.isValidJmsPropertyName("")).isFalse();
        assertThat(HeaderMapper.isValidJmsPropertyName(null)).isFalse();
    }

    @Test
    void truncatesOversizedAttributeValues() {
        String huge = "x".repeat(5000);
        Map<String, String> attrs = HeaderMapper.toAttributes(null, null, null, false, Map.of("note", huge));
        assertThat(attrs.get("note")).hasSize(1024);
    }

    @Test
    void neverWritesBlankValues() throws JMSException {
        TextMessage message = mock(TextMessage.class);
        HeaderMapper.applyToMessage(message, Map.of("correlationId", " ", "storeId", ""), null);
        verify(message, never()).setJMSCorrelationID(anyString());
        verify(message, never()).setStringProperty(anyString(), anyString());
    }
}
