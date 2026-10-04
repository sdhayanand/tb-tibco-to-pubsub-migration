package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;

/** One order line. {@code fulfillmentType} is STORE_PICKUP | SHIP_TO_HOME | ALTERATION. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderLine(
        int lineNumber,
        String sku,
        int quantity,
        BigDecimal unitPrice,
        String fulfillmentType,
        Alteration alteration) {

    public static final String FULFILLMENT_STORE_PICKUP = "STORE_PICKUP";
    public static final String FULFILLMENT_SHIP_TO_HOME = "SHIP_TO_HOME";
    public static final String FULFILLMENT_ALTERATION = "ALTERATION";
}
