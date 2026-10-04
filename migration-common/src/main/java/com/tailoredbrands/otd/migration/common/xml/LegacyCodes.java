package com.tailoredbrands.otd.migration.common.xml;

import com.tailoredbrands.otd.migration.common.model.OrderLine;

import java.util.Map;

/**
 * Code tables of the legacy TIBCO BW / OMS XML (single-letter codes) ⇄ canonical enums.
 *
 * <pre>
 *  OrderType : R=RETAIL  T=TAILORED  C=CUSTOM  X=RENTAL  E=ECOM
 *  FulfillType: P=STORE_PICKUP  S=SHIP_TO_HOME  A=ALTERATION
 * </pre>
 */
public final class LegacyCodes {

    public static final String CHANNEL_STORE = "STORE";
    public static final String CHANNEL_ECOM = "ECOM";

    private static final Map<String, String> ORDER_TYPE_FROM_CODE = Map.of(
            "R", "RETAIL",
            "T", "TAILORED",
            "C", "CUSTOM",
            "X", "RENTAL",
            "E", "ECOM");

    private static final Map<String, String> ORDER_TYPE_TO_CODE = Map.of(
            "RETAIL", "R",
            "TAILORED", "T",
            "CUSTOM", "C",
            "RENTAL", "X",
            "ECOM", "E");

    private static final Map<String, String> FULFILLMENT_FROM_CODE = Map.of(
            "P", OrderLine.FULFILLMENT_STORE_PICKUP,
            "S", OrderLine.FULFILLMENT_SHIP_TO_HOME,
            "A", OrderLine.FULFILLMENT_ALTERATION);

    private static final Map<String, String> FULFILLMENT_TO_CODE = Map.of(
            OrderLine.FULFILLMENT_STORE_PICKUP, "P",
            OrderLine.FULFILLMENT_SHIP_TO_HOME, "S",
            OrderLine.FULFILLMENT_ALTERATION, "A");

    private LegacyCodes() {
    }

    public static String orderTypeFromCode(String code) {
        return lookup(ORDER_TYPE_FROM_CODE, code, "OrderType");
    }

    public static String orderTypeToCode(String orderType) {
        return lookup(ORDER_TYPE_TO_CODE, orderType, "orderType");
    }

    public static String fulfillmentFromCode(String code) {
        return lookup(FULFILLMENT_FROM_CODE, code, "FulfillType");
    }

    public static String fulfillmentToCode(String fulfillmentType) {
        return lookup(FULFILLMENT_TO_CODE, fulfillmentType, "fulfillmentType");
    }

    /** The legacy XML has no channel; e-commerce orders are typed E, everything else came from a store. */
    public static String channelForOrderType(String orderType) {
        return "ECOM".equals(orderType) ? CHANNEL_ECOM : CHANNEL_STORE;
    }

    private static String lookup(Map<String, String> table, String key, String what) {
        if (key == null) {
            throw new LegacyXmlException("Missing " + what);
        }
        String normalized = key.trim().toUpperCase();
        String value = table.get(normalized);
        if (value == null) {
            throw new LegacyXmlException("Unknown " + what + " '" + key + "' (expected one of " + table.keySet() + ")");
        }
        return value;
    }
}
