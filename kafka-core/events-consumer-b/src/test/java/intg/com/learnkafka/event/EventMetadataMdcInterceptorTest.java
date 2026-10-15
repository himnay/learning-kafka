package com.learnkafka.event;

import com.learnkafka.AvroEvents;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

class EventMetadataMdcInterceptorTest {

    private final EventMetadataMdcInterceptor interceptor = new EventMetadataMdcInterceptor();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("Puts the record's base-event metadata into the MDC and clears it after the record")
    void putsAndClearsMetadata() {
        var metadata = EventMetadata.newEvent("LibraryEvent.NEW", "test", "req-7");
        var event = AvroEvents.withMetadata(AvroEvents.fromJson(
                "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1,\"bookName\":\"b\",\"bookAuthor\":\"a\"}}"), metadata);
        var record = new ConsumerRecord<Object, Object>("library-events", 0, 0L, null, event);

        interceptor.intercept(record, null);
        assertThat(MDC.get("eventId")).isEqualTo(metadata.eventId().toString());
        assertThat(MDC.get("eventType")).isEqualTo("LibraryEvent.NEW");
        assertThat(MDC.get("correlationId")).isEqualTo("req-7");

        interceptor.afterRecord(record, null);
        assertThat(MDC.get("eventId")).isNull();
        assertThat(MDC.get("correlationId")).isNull();
    }

    @Test
    @DisplayName("A record without metadata (written before it existed) leaves the MDC empty")
    void legacyRecord() {
        var record = new ConsumerRecord<Object, Object>("library-events", 0, 0L, null, AvroEvents.fromJson(
                "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1,\"bookName\":\"b\",\"bookAuthor\":\"a\"}}"));

        interceptor.intercept(record, null);
        assertThat(MDC.get("eventId")).isNull();
    }
}
