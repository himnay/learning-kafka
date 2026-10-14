package com.learnkafka.domain;

import com.learnkafka.event.BaseEvent;
import com.learnkafka.event.EventMetadata;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

/**
 * Child event of {@link BaseEvent}: the library event as the REST API accepts and returns it.
 * {@code metadata} is stamped by the controller (any value a client sends is replaced) and is
 * returned in the response, so callers learn the eventId.
 */
@Builder(toBuilder = true)
public record LibraryEvent(
        EventMetadata metadata,
        Integer libraryEventId,
        LibraryEventType libraryEventType,
        @NotNull @Valid Book book
) implements BaseEvent {

    public static final String TYPE_PREFIX = "LibraryEvent.";
}
