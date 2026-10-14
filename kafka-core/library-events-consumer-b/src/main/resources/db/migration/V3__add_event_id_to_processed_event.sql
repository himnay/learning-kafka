-- Idempotency by BUSINESS event id (the base event's metadata.eventId), on top of the
-- (group, topic, partition, offset) primary key: a producer that re-sends the same event lands it
-- at a NEW offset, which the offset key alone can't recognise. Events written before metadata
-- existed have no id (NULL) — a unique index allows any number of NULLs.
ALTER TABLE processed_event ADD COLUMN event_id UUID;

CREATE UNIQUE INDEX processed_event_group_event_id_uk ON processed_event (consumer_group, event_id);
