-- Coffee orders, owned by events-producer (schema coffee). An order and its event are written in ONE
-- transaction; Debezium reads the outbox INSERT from the write-ahead log and publishes it to Kafka.
CREATE TABLE coffee_order (
    id           UUID         PRIMARY KEY,
    name         VARCHAR(255) NOT NULL,
    nick_name    VARCHAR(255) NOT NULL,
    store_id     INTEGER      NOT NULL,
    status       VARCHAR(32)  NOT NULL,
    pick_up      VARCHAR(32)  NOT NULL,
    ordered_time TIMESTAMPTZ  NOT NULL
);

-- Transactional outbox. Column names id / aggregatetype / aggregateid / payload are the defaults of
-- Debezium's Outbox Event Router. Rows are inserted and deleted in the same transaction: the WAL keeps
-- the INSERT (which becomes the Kafka record), the router drops the DELETE, and the table stays empty.
CREATE TABLE outbox_event (
    id             UUID         PRIMARY KEY,   -- metadata.eventId; Debezium adds it as header "id"
    aggregatetype  VARCHAR(255) NOT NULL,      -- routes the event: "coffee-orders" = topic name
    aggregateid    VARCHAR(255) NOT NULL,      -- Kafka record key: the order id
    type           VARCHAR(255) NOT NULL,      -- "CoffeeOrder.PLACED" → header event-type
    payload        BYTEA        NOT NULL,      -- Confluent Avro wire format (magic byte + schema id + Avro binary)
    partition_no   INTEGER      NOT NULL,      -- Kafka partition chosen by the VIP rule
    correlation_id VARCHAR(255),               -- header correlation-id (from X-Correlation-Id), nullable
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
