package com.learnkafka.idempotency;

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

    private static final String INSERT_IF_ABSENT = """
            INSERT INTO processed_event (consumer_group, topic, partition_no, offset_no)
            VALUES (?, ?, ?, ?)
            ON CONFLICT DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Atomically records the record as processed.
     *
     * @return {@code true} the first time this record is seen, {@code false} for a redelivery
     */
    public boolean markProcessed(String consumerGroup, ConsumerRecord<?, ?> consumerRecord) {
        int inserted = jdbcTemplate.update(INSERT_IF_ABSENT, consumerGroup,
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
        return inserted == 1;
    }
}
