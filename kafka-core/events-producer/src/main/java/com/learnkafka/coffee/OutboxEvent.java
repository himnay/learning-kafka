package com.learnkafka.coffee;

import java.util.UUID;

/**
 * One row of {@code coffee.outbox_event}: everything Debezium's Outbox Event Router needs to build the
 * Kafka record.
 *
 * @param id            event id (metadata.eventId); becomes the record header {@code id}
 * @param aggregateType routes the event: the topic name
 * @param aggregateId   the record key
 * @param type          event type, becomes the header {@code event-type}
 * @param payload       the record value, already serialized (Confluent Avro wire format)
 * @param partition     the Kafka partition to write to
 * @param correlationId becomes the header {@code correlation-id}; may be null
 */
public record OutboxEvent(UUID id, String aggregateType, String aggregateId, String type, byte[] payload,
                          int partition, String correlationId) {}
