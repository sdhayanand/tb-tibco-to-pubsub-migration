package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.math.BigDecimal;

/** Alteration details for an ALTERATION line (hem, sleeve, waist ...). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Alteration(
        String type,
        BigDecimal measurementInches,
        String tailorShopId) {
}
