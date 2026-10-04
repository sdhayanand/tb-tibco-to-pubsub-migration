package com.tailoredbrands.otd.migration.common.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * The one ObjectMapper configuration used by every bridge (CONVENTIONS.md "JSON"):
 * ISO-8601 instants, nulls omitted (so proto-JSON schema validation accepts the payload),
 * unknown properties tolerated (additive schema evolution), BigDecimal written plain.
 */
public final class JsonMappers {

    private static final ObjectMapper CANONICAL = build();

    private JsonMappers() {
    }

    public static ObjectMapper canonical() {
        return CANONICAL;
    }

    public static ObjectMapper build() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }
}
