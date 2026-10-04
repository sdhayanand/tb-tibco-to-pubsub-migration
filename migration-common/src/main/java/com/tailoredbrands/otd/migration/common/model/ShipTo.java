package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Ship-to address for ship-to-home / ship-to-store orders. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ShipTo(
        String name,
        String line1,
        String line2,
        String city,
        String state,
        String postalCode,
        String country) {
}
