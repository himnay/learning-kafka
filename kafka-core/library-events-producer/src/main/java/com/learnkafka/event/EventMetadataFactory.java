package com.learnkafka.event;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Stamps the base-event metadata on every event this application publishes. */
@Component
public class EventMetadataFactory {

    private final String source;

    public EventMetadataFactory(@Value("${spring.application.name}") String source) {
        this.source = source;
    }

    /**
     * @param eventType     {@code <Aggregate>.<Action>}, e.g. {@code LibraryEvent.NEW}
     * @param correlationId from the request's {@code X-Correlation-Id} header; may be {@code null}
     */
    public EventMetadata newEvent(String eventType, String correlationId) {
        return EventMetadata.newEvent(eventType, source, correlationId);
    }
}
