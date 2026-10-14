package com.learnkafka.event;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Handles the base event's common properties ONCE for every child event type: before each record
 * reaches its listener, its metadata (eventId, eventType, correlationId) goes into the logging MDC,
 * so every log line written while processing it carries them (see logback-spring.xml). Cleared after
 * the record, whatever the outcome.
 */
public class EventMetadataMdcInterceptor implements RecordInterceptor<Object, Object> {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String CORRELATION_ID = "correlationId";

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        put(EventMetadata.of(record.value()).orElse(null));
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        clear();
    }

    /** Also used by listeners whose container has no interceptor support (share groups). */
    public static void put(EventMetadata metadata) {
        if (metadata == null) {
            clear();
            return;
        }
        MDC.put(EVENT_ID, metadata.eventId().toString());
        MDC.put(EVENT_TYPE, metadata.eventType());
        if (metadata.correlationId() != null) {
            MDC.put(CORRELATION_ID, metadata.correlationId());
        } else {
            MDC.remove(CORRELATION_ID);
        }
    }

    public static void clear() {
        MDC.remove(EVENT_ID);
        MDC.remove(EVENT_TYPE);
        MDC.remove(CORRELATION_ID);
    }
}
