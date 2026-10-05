package com.tailoredbrands.otd.migration.common.xml;

import com.fasterxml.jackson.databind.JsonNode;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.common.model.Alteration;
import com.tailoredbrands.otd.migration.common.model.Order;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.model.OrderLine;
import com.tailoredbrands.otd.migration.common.model.Rental;
import com.tailoredbrands.otd.migration.common.model.ShipTo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LegacyXmlMapperTest {

    private final LegacyXmlMapper mapper = new LegacyXmlMapper();

    private static String resource(String name) throws IOException {
        try (InputStream in = LegacyXmlMapperTest.class.getClassLoader().getResourceAsStream(name)) {
            assertThat(in).as("resource " + name).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void parsesLegacyTailoredOrder() throws IOException {
        Order order = mapper.parseOrder(resource("legacy-order-tailored.xml"));

        assertThat(order.orderId()).isEqualTo("ORD-2026-000123");
        assertThat(order.orderType()).isEqualTo("TAILORED");
        assertThat(order.channel()).isEqualTo("STORE");
        assertThat(order.storeId()).isEqualTo("0412");
        assertThat(order.customerId()).isEqualTo("C-77812");
        assertThat(order.orderedAt()).isEqualTo(Instant.parse("2026-10-03T22:14:00Z"));
        assertThat(order.promisedDate()).isEqualTo(LocalDate.of(2026, 10, 10));
        assertThat(order.currency()).isEqualTo("USD");
        assertThat(order.totalAmount()).isEqualByComparingTo("649.99");
        assertThat(order.lines()).hasSize(2);

        OrderLine suit = order.lines().get(0);
        assertThat(suit.lineNumber()).isEqualTo(1);
        assertThat(suit.sku()).isEqualTo("MW-SUIT-NAVY-42R");
        assertThat(suit.fulfillmentType()).isEqualTo("STORE_PICKUP");
        assertThat(suit.alteration()).isNull();

        OrderLine hem = order.lines().get(1);
        assertThat(hem.fulfillmentType()).isEqualTo("ALTERATION");
        assertThat(hem.alteration()).isNotNull();
        assertThat(hem.alteration().type()).isEqualTo("HEM");
        assertThat(hem.alteration().measurementInches()).isEqualByComparingTo("31.5");
        assertThat(hem.alteration().tailorShopId()).isEqualTo("TS-EASTBAY");
    }

    @Test
    void xmlToJsonToXmlRoundTrip() throws IOException {
        String xml = resource("legacy-order-tailored.xml");
        Instant eventTime = Instant.parse("2026-10-03T22:14:05.120Z");

        String json = mapper.toJson(xml, "ORDER_CREATED", eventTime, "store-0412-txn-889213", "ID:EMS.1A2B3C");
        JsonNode node = JsonMappers.canonical().readTree(json);

        // envelope matches ARCHITECTURE.md section 3.1
        assertThat(node.get("eventType").asText()).isEqualTo("ORDER_CREATED");
        assertThat(node.get("eventTime").asText()).isEqualTo("2026-10-03T22:14:05.120Z");
        assertThat(node.get("schemaVersion").asText()).isEqualTo("1");
        assertThat(node.get("source").asText()).isEqualTo("TIBCO_EMS_BRIDGE");
        assertThat(node.get("correlationId").asText()).isEqualTo("store-0412-txn-889213");
        assertThat(node.get("legacyMessageId").asText()).isEqualTo("ID:EMS.1A2B3C");
        assertThat(node.get("eventId").asText()).isNotBlank();
        assertThat(node.get("order").get("orderType").asText()).isEqualTo("TAILORED");
        assertThat(node.get("order").get("totalAmount").decimalValue()).isEqualByComparingTo("649.99");
        assertThat(node.get("order").get("lines").size()).isEqualTo(2);
        assertThat(node.get("order").get("lines").get(1).get("alteration").get("measurementInches").decimalValue())
                .isEqualByComparingTo("31.5");
        // nulls are omitted so the proto-JSON schema accepts the payload
        assertThat(node.get("order").has("rental")).isFalse();
        assertThat(node.get("order").has("shipTo")).isFalse();
        assertThat(node.has("fromLegacyBridge")).isFalse();

        // back to XML and parse again: same canonical order
        String xmlAgain = mapper.toLegacyXml(json);
        assertThat(xmlAgain).contains("<OrderNbr>ORD-2026-000123</OrderNbr>")
                .contains("<OrderType>T</OrderType>")
                .contains("<FulfillType>A</FulfillType>")
                .contains("<TotalAmt>649.99</TotalAmt>")
                .contains("<Measure>31.5</Measure>");
        Order original = mapper.parseOrder(xml);
        Order roundTripped = mapper.parseOrder(xmlAgain);
        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void canonicalToXmlToCanonicalRoundTripWithRentalAndShipTo() {
        Order order = new Order("ORD-2026-000777", "RENTAL", "STORE", "0088", "C-1",
                Instant.parse("2026-06-01T15:00:00Z"), LocalDate.of(2026, 6, 20), "USD", new BigDecimal("199.00"),
                List.of(new OrderLine(1, "RNT-TUX-BLACK-40R", 1, new BigDecimal("199.00"), "SHIP_TO_HOME", null)),
                new Rental("EVT-SMITH", LocalDate.of(2026, 6, 27), LocalDate.of(2026, 6, 29), "WED-SMITH-2026"),
                new ShipTo("John Smith", "1 Main St", "Apt 4", "Houston", "TX", "77002", "US"));
        OrderEvent event = OrderEvent.bridged(order, "ORDER_CREATED", Instant.now(), null, null);

        String xml = mapper.toXml(event);
        assertThat(xml).contains("<OrderType>X</OrderType>").contains("<GroupId>WED-SMITH-2026</GroupId>")
                .contains("<Zip>77002</Zip>").contains("<FulfillType>S</FulfillType>");

        Order parsed = mapper.parseOrder(xml);
        assertThat(parsed).isEqualTo(order);

        String json = mapper.toJson(event);
        OrderEvent back = mapper.fromJson(json);
        assertThat(back).isEqualTo(event);
    }

    @Test
    void acceptsNamespacePrefixedRootAndLegacyDateFormats() {
        String xml = """
                <ns0:Order xmlns:ns0="http://tailoredbrands.com/oms/order">
                  <ns0:OrderNbr>ORD-1</ns0:OrderNbr><ns0:OrderType>r</ns0:OrderType><ns0:StoreNbr>0001</ns0:StoreNbr>
                  <ns0:OrderDate>2026-10-03 10:11:12</ns0:OrderDate><ns0:PromiseDate>20261010</ns0:PromiseDate>
                  <ns0:TotalAmt>10</ns0:TotalAmt>
                  <ns0:Lines><ns0:Line><ns0:LineNbr>1</ns0:LineNbr><ns0:SKU>S</ns0:SKU><ns0:Qty>2</ns0:Qty>
                  <ns0:UnitPrice>5</ns0:UnitPrice><ns0:FulfillType>p</ns0:FulfillType></ns0:Line></ns0:Lines>
                </ns0:Order>
                """;
        Order order = mapper.parseOrder(xml);
        assertThat(order.orderType()).isEqualTo("RETAIL");
        assertThat(order.orderedAt()).isEqualTo(Instant.parse("2026-10-03T10:11:12Z"));
        assertThat(order.promisedDate()).isEqualTo(LocalDate.of(2026, 10, 10));
        assertThat(order.totalAmount()).isEqualTo(new BigDecimal("10.00"));
        assertThat(order.currency()).isEqualTo("USD");
        assertThat(order.lines().get(0).fulfillmentType()).isEqualTo("STORE_PICKUP");
    }

    @Test
    void rejectsPoisonPayloads() {
        assertThatThrownBy(() -> mapper.parseOrder("this is not xml"))
                .isInstanceOf(LegacyXmlException.class).hasMessageContaining("Malformed");
        assertThatThrownBy(() -> mapper.parseOrder("<Order><OrderNbr>1</OrderNbr></Order>"))
                .isInstanceOf(LegacyXmlException.class).hasMessageContaining("OrderType");
        assertThatThrownBy(() -> mapper.parseOrder("<Order><OrderNbr>1</OrderNbr><OrderType>Z</OrderType>"
                + "<StoreNbr>1</StoreNbr><Lines><Line/></Lines></Order>"))
                .isInstanceOf(LegacyXmlException.class).hasMessageContaining("Unknown OrderType");
        assertThatThrownBy(() -> mapper.parseOrder("<Wrapper><Nope/></Wrapper>"))
                .isInstanceOf(LegacyXmlException.class).hasMessageContaining("No <Order>");
        assertThatThrownBy(() -> mapper.fromJson("{\"eventType\":\"ORDER_CREATED\"}"))
                .isInstanceOf(LegacyXmlException.class).hasMessageContaining("order");
        assertThatThrownBy(() -> mapper.parseOrder("<!DOCTYPE foo [<!ENTITY xxe SYSTEM \"file:///etc/passwd\">]>"
                + "<Order><OrderNbr>&xxe;</OrderNbr></Order>"))
                .isInstanceOf(LegacyXmlException.class);
    }

    @Test
    void alterationMeasurementRoundTripsAsBigDecimal() {
        Alteration a = new Alteration("SLEEVE", new BigDecimal("0.25"), "TS-1");
        OrderLine line = new OrderLine(1, "ALT", 1, new BigDecimal("15.00"), "ALTERATION", a);
        Order order = new Order("O", "TAILORED", "STORE", "0001", null, Instant.parse("2026-01-01T00:00:00Z"), null,
                "USD", new BigDecimal("15.00"), List.of(line), null, null);
        assertThat(mapper.parseOrder(mapper.toXml(order))).isEqualTo(order);
    }

    @Test
    void acceptsBareXsDateOrderDateFromTheOmsContract() {
        // Found in the GCP smoke test: the EMS publisher sends <OrderDate>2026-10-05</OrderDate> (xs:date)
        assertThat(LegacyXmlMapper.parseInstant("2026-10-05", "OrderDate"))
                .isEqualTo(Instant.parse("2026-10-05T00:00:00Z"));
        assertThat(LegacyXmlMapper.parseInstant("2026-10-05T08:15:00Z", "OrderDate"))
                .isEqualTo(Instant.parse("2026-10-05T08:15:00Z"));
    }
}
