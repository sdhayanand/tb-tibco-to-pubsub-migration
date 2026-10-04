package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * Canonical order (ARCHITECTURE.md section 3.1, {@code order} object).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Order(
        String orderId,
        String orderType,
        String channel,
        String storeId,
        String customerId,
        Instant orderedAt,
        LocalDate promisedDate,
        String currency,
        BigDecimal totalAmount,
        List<OrderLine> lines,
        Rental rental,
        ShipTo shipTo) {

    public Order {
        lines = lines == null ? List.of() : List.copyOf(lines);
    }

    public int lineCount() {
        return lines.size();
    }
}
