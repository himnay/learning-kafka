package com.learnkafka.coffee;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Writes an event to the transactional outbox, inside the caller's transaction.
 *
 * The row is inserted and deleted again before the transaction commits. Debezium reads both changes from
 * the write-ahead log: the INSERT becomes the Kafka record, the DELETE is dropped by the Outbox Event
 * Router. The table therefore stays empty — nothing to clean up, and a connector snapshot can never
 * re-send old events.
 */
@Repository
@RequiredArgsConstructor
public class OutboxRepository {

    private final JdbcClient jdbcClient;

    public void append(OutboxEvent event) {
        jdbcClient.sql("""
                        INSERT INTO coffee.outbox_event (id, aggregatetype, aggregateid, type, payload, partition_no, correlation_id)
                        VALUES (:id, :aggregateType, :aggregateId, :type, :payload, :partition, :correlationId)
                        """)
                .param("id", event.id())
                .param("aggregateType", event.aggregateType())
                .param("aggregateId", event.aggregateId())
                .param("type", event.type())
                .param("payload", event.payload())
                .param("partition", event.partition())
                .param("correlationId", event.correlationId())
                .update();
        jdbcClient.sql("DELETE FROM coffee.outbox_event WHERE id = :id")
                .param("id", event.id())
                .update();
    }
}
