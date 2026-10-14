-- Reconciliation: what the producer SENT (events, written by the JDBC sink connector)
-- vs what the consumers PROCESSED (processed_event, written by consumer-a/-b in the same
-- DB transaction as the business change). Both are keyed by Kafka (topic, partition, offset).
-- Run: docker exec -i kafka-core-postgres psql -U library -d library < connect/reconciliation.sql

-- 1. Summary per partition
SELECT e.kafka_partition                          AS partition,
       count(*)                                    AS sent,
       count(p.offset_no)                          AS processed,
       count(*) - count(p.offset_no)               AS not_processed
FROM events e
LEFT JOIN processed_event p
       ON p.consumer_group = 'library-events-listener-group'
      AND p.topic        = e.kafka_topic
      AND p.partition_no = e.kafka_partition
      AND p.offset_no    = e.kafka_offset
GROUP BY e.kafka_partition
ORDER BY e.kafka_partition;

-- 2. Sent but never processed: dead-lettered (see library-events.DLT), still in flight,
--    or sent before the consumers started recording processed_event
SELECT e.kafka_partition, e.kafka_offset, e.kafka_timestamp,
       e.library_event_type, e.library_event_id, e.book_id, e.book_name
FROM events e
LEFT JOIN processed_event p
       ON p.consumer_group = 'library-events-listener-group'
      AND p.topic        = e.kafka_topic
      AND p.partition_no = e.kafka_partition
      AND p.offset_no    = e.kafka_offset
WHERE p.offset_no IS NULL
ORDER BY e.kafka_partition, e.kafka_offset;

-- 3. Processed but missing from the replica (should be empty; non-empty = the connector is behind)
SELECT p.topic, p.partition_no, p.offset_no, p.processed_at
FROM processed_event p
LEFT JOIN events e
       ON e.kafka_topic = p.topic AND e.kafka_partition = p.partition_no AND e.kafka_offset = p.offset_no
WHERE p.consumer_group = 'library-events-listener-group'
  AND p.topic = 'library-events'
  AND e.kafka_offset IS NULL;
