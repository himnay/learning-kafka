package com.learnkafka.event;

import java.util.Objects;

/**
 * Consumer-side child event: any received Avro event (LibraryEventAvro, CoffeeOrder, ...) together with
 * its common metadata, so listeners can treat every event type through the {@link BaseEvent} view.
 *
 * @param metadata the event's common properties, or {@code null} for events written before metadata existed
 * @param payload  the typed Avro event
 * @param <T>      the generated Avro class
 */
public record ReceivedEvent<T>(EventMetadata metadata, T payload) implements BaseEvent {

    public ReceivedEvent {
        Objects.requireNonNull(payload, "payload");
    }

    public static <T> ReceivedEvent<T> of(T payload) {
        return new ReceivedEvent<>(EventMetadata.of(payload).orElse(null), payload);
    }
}
