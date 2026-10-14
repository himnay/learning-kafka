package com.learnkafka.idempotency;

import com.learnkafka.event.EventMetadata;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Idempotent-consumer store: remembers which Kafka records were already applied.
 * Runs inside the caller's database transaction (JdbcTemplate joins the JPA transaction).
 */
@Repository
@RequiredArgsConstructor
public class ProcessedEventRepository {

    /**
     * ON CONFLICT DO NOTHING without a target covers EVERY unique constraint: the primary key
     * (same delivery position = redelivery) and the (consumer_group, event_id) index (same business
     * event re-sent at another offset).
     */
    private static final String INSERT_IF_ABSENT = """
            INSERT INTO processed_event (consumer_group, topic, partition_no, offset_no, event_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Atomically records the record as processed.
     *
     * @return {@code true} the first time this record (and its eventId) is seen; {@code false} for a
     *         redelivery or a re-sent copy of an event that was already applied
     */
    public boolean markProcessed(String consumerGroup, ConsumerRecord<?, ?> consumerRecord) {
        var eventId = EventMetadata.of(consumerRecord.value()).map(EventMetadata::eventId).orElse(null);
        int inserted = jdbcTemplate.update(INSERT_IF_ABSENT, consumerGroup,
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), eventId);
        return inserted == 1;
    }
}
