package com.tailoredbrands.otd.migration.common.xml;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Exposes the (stateless, thread-safe) {@link LegacyXmlMapper} as a bean. */
@Configuration(proxyBeanMethods = false)
public class LegacyXmlConfig {

    @Bean
    public LegacyXmlMapper legacyXmlMapper() {
        return new LegacyXmlMapper();
    }
}
