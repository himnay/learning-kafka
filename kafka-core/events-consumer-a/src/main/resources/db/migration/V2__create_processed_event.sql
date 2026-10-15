-- Idempotent consumer: one row per Kafka record already applied to the database.
-- Written in the SAME database transaction as the business change, so a record is
-- either fully processed and marked, or neither. A redelivered record (same group,
-- topic, partition, offset) hits the primary key and is skipped.
CREATE TABLE processed_event (
    consumer_group VARCHAR(255) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_no   INTEGER      NOT NULL,
    offset_no      BIGINT       NOT NULL,
    processed_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition_no, offset_no)
);
