package com.learnkafka.event;

import java.time.Instant;
import java.util.UUID;

/**
 * The base event: every event — library events, coffee orders, on the producer and on the consumer
 * side — carries the same {@link EventMetadata}. Child events implement this interface (Java records
 * can't extend a class, so the common part is composed, exactly like the Avro schemas do).
 */
public interface BaseEvent {

    /** Common properties; {@code null} only for events written before metadata existed. */
    EventMetadata metadata();

    default UUID eventId() {
        return metadata() != null ? metadata().eventId() : null;
    }

    default String eventType() {
        return metadata() != null ? metadata().eventType() : null;
    }

    default Instant occurredAt() {
        return metadata() != null ? metadata().occurredAt() : null;
    }
}
