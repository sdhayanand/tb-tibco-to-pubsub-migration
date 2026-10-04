package com.tailoredbrands.otd.migration.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDate;

/** Tuxedo rental details (wedding party linkage, event and return dates). */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Rental(
        String eventId,
        LocalDate eventDate,
        LocalDate returnDueDate,
        String groupId) {
}
