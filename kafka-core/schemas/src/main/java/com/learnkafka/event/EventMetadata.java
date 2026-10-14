package com.learnkafka.event;

import com.learnkafka.avro.EventMetadataAvro;
import org.apache.avro.generic.GenericRecord;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Common properties of every event — the Java side of the "base event". Shared by the producer and
 * the consumers (this is the contract module), and mapped 1:1 to the Avro {@link EventMetadataAvro}
 * that each event schema embeds as its {@code metadata} field.
 *
 * @param eventId       unique per event; consumers dedupe on it
 * @param eventType     {@code <Aggregate>.<Action>}, e.g. {@code LibraryEvent.NEW}
 * @param eventVersion  version of the event's payload contract
 * @param occurredAt    when the event happened (millisecond precision, as stored in Avro)
 * @param source        producing application
 * @param correlationId ties the event to the request that caused it; may be {@code null}
 */
public record EventMetadata(UUID eventId,
                            String eventType,
                            int eventVersion,
                            Instant occurredAt,
                            String source,
                            String correlationId) {

    /** Avro field name every event uses for its metadata. */
    public static final String FIELD = "metadata";

    public EventMetadata {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(source, "source");
    }

    /** Metadata for a brand-new event: random id, now, version 1. */
    public static EventMetadata newEvent(String eventType, String source, String correlationId) {
        return new EventMetadata(UUID.randomUUID(), eventType, 1,
                Instant.now().truncatedTo(ChronoUnit.MILLIS), source, correlationId);
    }

    public EventMetadataAvro toAvro() {
        return EventMetadataAvro.newBuilder()
                .setEventId(eventId)
                .setEventType(eventType)
                .setEventVersion(eventVersion)
                .setOccurredAt(occurredAt)
                .setSource(source)
                .setCorrelationId(correlationId)
                .build();
    }

    public static EventMetadata fromAvro(EventMetadataAvro avro) {
        return new EventMetadata(avro.getEventId(), avro.getEventType().toString(), avro.getEventVersion(),
                avro.getOccurredAt(), avro.getSource().toString(),
                avro.getCorrelationId() != null ? avro.getCorrelationId().toString() : null);
    }

    /**
     * Reads the metadata of ANY event record — every generated event class is a {@link GenericRecord}
     * with a {@code metadata} field — so common handling (logging, idempotency) works for all event
     * types. Empty for records written before the field existed (it is optional in the schema).
     */
    public static Optional<EventMetadata> of(Object event) {
        if (event instanceof GenericRecord record
                && record.getSchema().getField(FIELD) != null
                && record.get(FIELD) instanceof EventMetadataAvro avro) {
            return Optional.of(fromAvro(avro));
        }
        return Optional.empty();
    }
}
