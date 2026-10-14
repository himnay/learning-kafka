package com.learnkafka.event;

import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.avro.BookAvro;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.avro.LibraryEventTypeAvro;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class EventMetadataTest {

    @Test
    @DisplayName("newEvent stamps a random id, millisecond time and version 1")
    void newEvent() {
        var metadata = EventMetadata.newEvent("LibraryEvent.NEW", "library-events-producer", "req-1");

        assertThat(metadata.eventId()).isNotNull();
        assertThat(metadata.eventVersion()).isEqualTo(1);
        assertThat(metadata.occurredAt()).isEqualTo(metadata.occurredAt().truncatedTo(ChronoUnit.MILLIS));
        assertThat(EventMetadata.newEvent("x", "y", null).eventId()).isNotEqualTo(metadata.eventId());
    }

    @Test
    @DisplayName("Java ↔ Avro round trip keeps every field (Avro stores timestamp-millis)")
    void avroRoundTrip() {
        var metadata = EventMetadata.newEvent("CoffeeOrder.PLACED", "library-events-producer", null);

        assertThat(EventMetadata.fromAvro(metadata.toAvro())).isEqualTo(metadata);
    }

    @Test
    @DisplayName("of() reads the metadata from ANY event record, and is empty for events written before it existed")
    void readsMetadataFromAnyEvent() {
        var metadata = EventMetadata.newEvent("LibraryEvent.NEW", "test", "req-9");
        var withMetadata = libraryEvent().setMetadata(metadata.toAvro()).build();
        var legacy = libraryEvent().build();   // metadata = null (schema default)

        assertThat(EventMetadata.of(withMetadata)).contains(metadata);
        assertThat(EventMetadata.of(legacy)).isEmpty();
        assertThat(EventMetadata.of("not an event")).isEmpty();
        assertThat(EventMetadata.of(null)).isEmpty();
        assertThat(CoffeeOrder.getClassSchema().getField(EventMetadata.FIELD)).as("coffee orders carry it too").isNotNull();
    }

    @Test
    @DisplayName("ReceivedEvent exposes the base-event view of a received child event")
    void receivedEvent() {
        var metadata = EventMetadata.newEvent("LibraryEvent.UPDATE", "test", null);
        var received = ReceivedEvent.of(libraryEvent().setMetadata(metadata.toAvro()).build());

        assertThat(received.eventId()).isEqualTo(metadata.eventId());
        assertThat(received.eventType()).isEqualTo("LibraryEvent.UPDATE");
        assertThat(ReceivedEvent.of(libraryEvent().build()).eventId()).isNull();
    }

    @Test
    @DisplayName("Required metadata fields are enforced")
    void requiredFields() {
        assertThatNullPointerException().isThrownBy(() ->
                new EventMetadata(null, "t", 1, Instant.now(), "s", null));
    }

    private static LibraryEventAvro.Builder libraryEvent() {
        return LibraryEventAvro.newBuilder()
                .setLibraryEventType(LibraryEventTypeAvro.NEW)
                .setBook(BookAvro.newBuilder().setBookId(1).setBookName("b").setBookAuthor("a").build());
    }
}
