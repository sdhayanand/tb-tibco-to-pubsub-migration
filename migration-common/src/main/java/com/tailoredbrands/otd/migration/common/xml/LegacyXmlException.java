package com.tailoredbrands.otd.migration.common.xml;

/** Thrown when a legacy XML payload cannot be parsed or mapped (poison message → DLQ). */
public class LegacyXmlException extends RuntimeException {

    public LegacyXmlException(String message) {
        super(message);
    }

    public LegacyXmlException(String message, Throwable cause) {
        super(message, cause);
    }
}
