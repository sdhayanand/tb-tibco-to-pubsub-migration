package com.tailoredbrands.otd.migration.common.xml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tailoredbrands.otd.migration.common.json.JsonMappers;
import com.tailoredbrands.otd.migration.common.model.Alteration;
import com.tailoredbrands.otd.migration.common.model.Order;
import com.tailoredbrands.otd.migration.common.model.OrderEvent;
import com.tailoredbrands.otd.migration.common.model.OrderLine;
import com.tailoredbrands.otd.migration.common.model.Rental;
import com.tailoredbrands.otd.migration.common.model.ShipTo;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Hand-written mapper between the legacy order XML that TIBCO BusinessWorks puts on
 * {@code TB.ORDERS.OUT} (and that the ERP expects on {@code ERP.ORDERS.IN}) and the canonical
 * {@link OrderEvent} JSON.
 *
 * <p>Legacy format (the order-intake XSD of {@code tb-integration-services}):</p>
 * <pre>{@code
 * <Order>
 *   <OrderNbr>ORD-2026-000123</OrderNbr>
 *   <OrderType>T</OrderType>                 R|T|C|X|E
 *   <StoreNbr>0412</StoreNbr>
 *   <CustNbr>C-77812</CustNbr>
 *   <OrderDate>2026-10-03T22:14:00Z</OrderDate>
 *   <PromiseDate>2026-10-10</PromiseDate>
 *   <Currency>USD</Currency>
 *   <TotalAmt>649.99</TotalAmt>
 *   <Lines>
 *     <Line>
 *       <LineNbr>1</LineNbr><SKU>MW-SUIT-NAVY-42R</SKU><Qty>1</Qty><UnitPrice>599.99</UnitPrice>
 *       <FulfillType>P</FulfillType>         P|S|A
 *       <Alteration><AltType>HEM</AltType><Measure>31.5</Measure><TailorShop>TS-EASTBAY</TailorShop></Alteration>
 *     </Line>
 *   </Lines>
 *   <Rental><EventDate/><GroupId/><ReturnDate/></Rental>
 *   <ShipTo><Name/><Addr1/><Addr2/><City/><State/><Zip/><Country/></ShipTo>
 * </Order>
 * }</pre>
 *
 * <p>Parsing uses the JDK DOM parser with DTDs and external entities disabled (XXE-safe);
 * writing uses {@link XMLStreamWriter}. Namespace prefixes that BW sometimes adds
 * ({@code <ns0:Order xmlns:ns0=...>}) are ignored by matching on local names. The class is
 * thread-safe and stateless.</p>
 */
public final class LegacyXmlMapper {

    public static final String ROOT_ELEMENT = "Order";

    private static final ErrorHandler QUIET_ERROR_HANDLER = new ErrorHandler() {
        @Override
        public void warning(SAXParseException e) {
            // ignore
        }

        @Override
        public void error(SAXParseException e) throws SAXException {
            throw e;
        }

        @Override
        public void fatalError(SAXParseException e) throws SAXException {
            throw e;
        }
    };

    private static final DateTimeFormatter LEGACY_DATETIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter LEGACY_COMPACT_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final ObjectMapper json;

    public LegacyXmlMapper() {
        this(JsonMappers.canonical());
    }

    public LegacyXmlMapper(ObjectMapper json) {
        this.json = json;
    }

    // ------------------------------------------------------------------ XML -> canonical

    /** Parses legacy XML into an {@link Order}. */
    public Order parseOrder(String xml) {
        if (xml == null || xml.isBlank()) {
            throw new LegacyXmlException("Empty XML payload");
        }
        Element root = findOrderElement(parse(xml));
        if (root == null) {
            throw new LegacyXmlException("No <" + ROOT_ELEMENT + "> element found");
        }
        String orderId = requiredText(root, "OrderNbr");
        String orderType = LegacyCodes.orderTypeFromCode(requiredText(root, "OrderType"));
        String storeId = requiredText(root, "StoreNbr");
        String customerId = text(root, "CustNbr");
        Instant orderedAt = parseInstant(text(root, "OrderDate"), "OrderDate");
        LocalDate promisedDate = parseDate(text(root, "PromiseDate"), "PromiseDate");
        String currency = text(root, "Currency");
        BigDecimal totalAmount = parseMoney(text(root, "TotalAmt"), "TotalAmt");

        List<OrderLine> lines = new ArrayList<>();
        Element linesEl = child(root, "Lines");
        if (linesEl != null) {
            for (Element lineEl : children(linesEl, "Line")) {
                lines.add(parseLine(lineEl));
            }
        }
        if (lines.isEmpty()) {
            throw new LegacyXmlException("Order " + orderId + " has no <Line> elements");
        }

        Rental rental = parseRental(child(root, "Rental"));
        ShipTo shipTo = parseShipTo(child(root, "ShipTo"));

        return new Order(orderId, orderType, LegacyCodes.channelForOrderType(orderType), storeId, customerId,
                orderedAt, promisedDate, currency == null ? "USD" : currency, totalAmount, lines, rental, shipTo);
    }

    /** Parses legacy XML and wraps it in a bridged {@link OrderEvent} envelope. */
    public OrderEvent toEvent(String xml, String eventType, Instant eventTime, String correlationId,
                              String legacyMessageId) {
        return OrderEvent.bridged(parseOrder(xml), eventType, eventTime, correlationId, legacyMessageId);
    }

    /** Legacy XML → canonical JSON string. */
    public String toJson(String xml, String eventType, Instant eventTime, String correlationId,
                         String legacyMessageId) {
        return toJson(toEvent(xml, eventType, eventTime, correlationId, legacyMessageId));
    }

    public String toJson(OrderEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new LegacyXmlException("Cannot serialize OrderEvent", e);
        }
    }

    public OrderEvent fromJson(String canonicalJson) {
        try {
            OrderEvent event = json.readValue(canonicalJson, OrderEvent.class);
            if (event.order() == null) {
                throw new LegacyXmlException("Canonical event has no 'order' object");
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new LegacyXmlException("Cannot parse canonical OrderEvent JSON: " + e.getOriginalMessage(), e);
        }
    }

    // ------------------------------------------------------------------ canonical -> XML

    /** Canonical JSON string → legacy XML. */
    public String toLegacyXml(String canonicalJson) {
        return toXml(fromJson(canonicalJson).order());
    }

    public String toXml(OrderEvent event) {
        return toXml(event.order());
    }

    /** Writes the legacy XML for an order. */
    public String toXml(Order order) {
        if (order == null) {
            throw new LegacyXmlException("order is null");
        }
        StringWriter out = new StringWriter();
        try {
            // A factory per call: XMLOutputFactory instances are not guaranteed to be thread-safe.
            XMLStreamWriter w = XMLOutputFactory.newFactory().createXMLStreamWriter(out);
            w.writeStartDocument("UTF-8", "1.0");
            w.writeStartElement(ROOT_ELEMENT);
            element(w, "OrderNbr", order.orderId());
            element(w, "OrderType", LegacyCodes.orderTypeToCode(order.orderType()));
            element(w, "StoreNbr", order.storeId());
            element(w, "CustNbr", order.customerId());
            element(w, "OrderDate", order.orderedAt() == null ? null : DateTimeFormatter.ISO_INSTANT.format(order.orderedAt()));
            element(w, "PromiseDate", order.promisedDate() == null ? null : order.promisedDate().toString());
            element(w, "Currency", order.currency());
            element(w, "TotalAmt", money(order.totalAmount()));
            w.writeStartElement("Lines");
            for (OrderLine line : order.lines()) {
                writeLine(w, line);
            }
            w.writeEndElement();
            if (order.rental() != null) {
                Rental r = order.rental();
                w.writeStartElement("Rental");
                element(w, "EventId", r.eventId());
                element(w, "EventDate", r.eventDate() == null ? null : r.eventDate().toString());
                element(w, "ReturnDate", r.returnDueDate() == null ? null : r.returnDueDate().toString());
                element(w, "GroupId", r.groupId());
                w.writeEndElement();
            }
            if (order.shipTo() != null) {
                ShipTo s = order.shipTo();
                w.writeStartElement("ShipTo");
                element(w, "Name", s.name());
                element(w, "Addr1", s.line1());
                element(w, "Addr2", s.line2());
                element(w, "City", s.city());
                element(w, "State", s.state());
                element(w, "Zip", s.postalCode());
                element(w, "Country", s.country());
                w.writeEndElement();
            }
            w.writeEndElement();
            w.writeEndDocument();
            w.flush();
            w.close();
        } catch (XMLStreamException e) {
            throw new LegacyXmlException("Cannot write legacy XML for order " + order.orderId(), e);
        }
        return out.toString();
    }

    // ------------------------------------------------------------------ internals: parse

    private OrderLine parseLine(Element lineEl) {
        int lineNumber = parseInt(requiredText(lineEl, "LineNbr"), "LineNbr");
        String sku = requiredText(lineEl, "SKU");
        int qty = parseInt(requiredText(lineEl, "Qty"), "Qty");
        BigDecimal unitPrice = parseMoney(text(lineEl, "UnitPrice"), "UnitPrice");
        String fulfillmentType = LegacyCodes.fulfillmentFromCode(requiredText(lineEl, "FulfillType"));
        Alteration alteration = null;
        Element altEl = child(lineEl, "Alteration");
        if (altEl != null) {
            String measure = text(altEl, "Measure");
            alteration = new Alteration(
                    text(altEl, "AltType"),
                    measure == null ? null : parseDecimal(measure, "Measure"),
                    text(altEl, "TailorShop"));
        }
        return new OrderLine(lineNumber, sku, qty, unitPrice, fulfillmentType, alteration);
    }

    private Rental parseRental(Element el) {
        if (el == null) {
            return null;
        }
        return new Rental(
                text(el, "EventId"),
                parseDate(text(el, "EventDate"), "EventDate"),
                parseDate(text(el, "ReturnDate"), "ReturnDate"),
                text(el, "GroupId"));
    }

    private ShipTo parseShipTo(Element el) {
        if (el == null) {
            return null;
        }
        return new ShipTo(text(el, "Name"), text(el, "Addr1"), text(el, "Addr2"), text(el, "City"),
                text(el, "State"), text(el, "Zip"), text(el, "Country"));
    }

    private Document parse(String xml) {
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(false);
            dbf.setXIncludeAware(false);
            dbf.setExpandEntityReferences(false);
            dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
            dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            DocumentBuilder builder = dbf.newDocumentBuilder();
            builder.setErrorHandler(QUIET_ERROR_HANDLER); // no "[Fatal Error]" noise on stderr; errors surface as exceptions
            return builder.parse(new InputSource(new StringReader(xml)));
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException("XML parser misconfigured", e);
        } catch (Exception e) {
            throw new LegacyXmlException("Malformed legacy XML: " + e.getMessage(), e);
        }
    }

    private static Element findOrderElement(Document doc) {
        Element root = doc.getDocumentElement();
        if (root == null) {
            return null;
        }
        if (ROOT_ELEMENT.equals(localName(root))) {
            return root;
        }
        return findDescendant(root, ROOT_ELEMENT);
    }

    private static Element findDescendant(Element parent, String name) {
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n instanceof Element el) {
                if (name.equals(localName(el))) {
                    return el;
                }
                Element deeper = findDescendant(el, name);
                if (deeper != null) {
                    return deeper;
                }
            }
        }
        return null;
    }

    private static String localName(Node node) {
        String name = node.getNodeName();
        int colon = name.indexOf(':');
        return colon < 0 ? name : name.substring(colon + 1);
    }

    private static Element child(Element parent, String name) {
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n instanceof Element el && name.equals(localName(el))) {
                return el;
            }
        }
        return null;
    }

    private static List<Element> children(Element parent, String name) {
        List<Element> result = new ArrayList<>();
        NodeList nodes = parent.getChildNodes();
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n instanceof Element el && name.equals(localName(el))) {
                result.add(el);
            }
        }
        return result;
    }

    private static String text(Element parent, String name) {
        Element el = child(parent, name);
        if (el == null) {
            return null;
        }
        String value = el.getTextContent();
        if (value == null) {
            return null;
        }
        value = value.trim();
        return value.isEmpty() ? null : value;
    }

    private static String requiredText(Element parent, String name) {
        String value = text(parent, name);
        if (value == null) {
            throw new LegacyXmlException("Missing required element <" + name + "> under <" + localName(parent) + ">");
        }
        return value;
    }

    private static int parseInt(String value, String what) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new LegacyXmlException("Invalid integer in <" + what + ">: " + value, e);
        }
    }

    private static BigDecimal parseDecimal(String value, String what) {
        try {
            return new BigDecimal(value.trim());
        } catch (NumberFormatException e) {
            throw new LegacyXmlException("Invalid decimal in <" + what + ">: " + value, e);
        }
    }

    private static BigDecimal parseMoney(String value, String what) {
        if (value == null) {
            return null;
        }
        return parseDecimal(value, what).setScale(2, RoundingMode.HALF_UP);
    }

    private static String money(BigDecimal value) {
        return value == null ? null : value.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }

    /** Accepts RFC-3339 ("2026-10-03T22:14:00Z"), ISO local date-time and "yyyy-MM-dd HH:mm:ss" (both as UTC). */
    static Instant parseInstant(String value, String what) {
        if (value == null) {
            return null;
        }
        String v = value.trim();
        try {
            return Instant.parse(v);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDateTime.parse(v).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDateTime.parse(v, LEGACY_DATETIME).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw new LegacyXmlException("Invalid timestamp in <" + what + ">: " + value, e);
        }
    }

    /** Accepts "yyyy-MM-dd" and "yyyyMMdd". */
    static LocalDate parseDate(String value, String what) {
        if (value == null) {
            return null;
        }
        String v = value.trim();
        try {
            return LocalDate.parse(v);
        } catch (DateTimeParseException ignored) {
            // fall through
        }
        try {
            return LocalDate.parse(v, LEGACY_COMPACT_DATE);
        } catch (DateTimeParseException e) {
            throw new LegacyXmlException("Invalid date in <" + what + ">: " + value, e);
        }
    }

    // ------------------------------------------------------------------ internals: write

    private static void writeLine(XMLStreamWriter w, OrderLine line) throws XMLStreamException {
        w.writeStartElement("Line");
        element(w, "LineNbr", Integer.toString(line.lineNumber()));
        element(w, "SKU", line.sku());
        element(w, "Qty", Integer.toString(line.quantity()));
        element(w, "UnitPrice", money(line.unitPrice()));
        element(w, "FulfillType", LegacyCodes.fulfillmentToCode(line.fulfillmentType()));
        if (line.alteration() != null) {
            Alteration a = line.alteration();
            w.writeStartElement("Alteration");
            element(w, "AltType", a.type());
            element(w, "Measure", a.measurementInches() == null ? null : a.measurementInches().toPlainString());
            element(w, "TailorShop", a.tailorShopId());
            w.writeEndElement();
        }
        w.writeEndElement();
    }

    private static void element(XMLStreamWriter w, String name, String value) throws XMLStreamException {
        if (value == null) {
            return;
        }
        w.writeStartElement(name);
        w.writeCharacters(value);
        w.writeEndElement();
    }
}
