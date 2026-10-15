package com.learnkafka.controller;

import com.learnkafka.TestcontainersConfiguration;
import org.springframework.context.annotation.Import;
import com.learnkafka.avro.BookAvro;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.avro.LibraryEventTypeAvro;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestClient;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        // Confluent's in-memory Schema Registry, shared by every client using the same mock:// scope
        "spring.kafka.producer.properties.schema.registry.url=" + LibraryEventsControllerIntegrationTest.REGISTRY
})
class LibraryEventsControllerIntegrationTest {

    static final String REGISTRY = "mock://events-producer-test";

    @LocalServerPort
    int port;

    RestClient restClient;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, LibraryEventAvro> consumer;

    @BeforeEach
    void setUp() {
        restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
        var configs = new HashMap<>(KafkaTestUtils.consumerProps(embeddedKafkaBroker, "group1", true));
        // DefaultKafkaConsumerFactory configures the deserializer from these props
        configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        var avroDeserializer = new KafkaAvroDeserializer();
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(),
                (Deserializer<LibraryEventAvro>) (Deserializer<?>) avroDeserializer)
                .createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @DisplayName("POSTing a new library event returns 201 Created and publishes a NEW event to the library-events topic")
    void postLibraryEvent_publishes_newEvent() {
        var book  = Book.builder().bookId(123).bookAuthor("Dilip").bookName("Kafka using Spring Boot").build();
        var event = LibraryEvent.builder().libraryEventId(null).book(book).build();

        ResponseEntity<Void> response = restClient.post()
                .uri("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .body(event)
                .retrieve()
                .toBodilessEntity();

        assertEquals(HttpStatus.CREATED, response.getStatusCode());

        var record = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        assertEquals(avroEvent(null, "NEW", 123), withoutMetadata(record.value()));
        assertMetadata(record.value(), "LibraryEvent.NEW");
    }

    @Test
    @DisplayName("PUTting a library event update returns 200 OK and publishes an UPDATE event to the library-events topic")
    void putLibraryEvent_publishes_updateEvent() {
        var book  = Book.builder().bookId(456).bookAuthor("Dilip").bookName("Kafka using Spring Boot").build();
        var event = LibraryEvent.builder().libraryEventId(123).book(book).build();

        ResponseEntity<Void> response = restClient.put()
                .uri("/v1/libraryevent")
                .contentType(MediaType.APPLICATION_JSON)
                .body(event)
                .retrieve()
                .toBodilessEntity();

        assertEquals(HttpStatus.OK, response.getStatusCode());

        var record = KafkaTestUtils.getSingleRecord(consumer, "library-events");
        assertEquals(avroEvent(123, "UPDATE", 456), withoutMetadata(record.value()));
        assertMetadata(record.value(), "LibraryEvent.UPDATE");
    }

    /** The Avro record the consumer should read back (Avro records implement equals()). */
    private static LibraryEventAvro avroEvent(Integer id, String type, int bookId) {
        return LibraryEventAvro.newBuilder()
                .setLibraryEventId(id)
                .setLibraryEventType(LibraryEventTypeAvro.valueOf(type))
                .setBook(BookAvro.newBuilder()
                        .setBookId(bookId).setBookName("Kafka using Spring Boot").setBookAuthor("Dilip").build())
                .build();
    }

    /** The base-event metadata is stamped by the producer: random id, current time, the app as source. */
    private static void assertMetadata(LibraryEventAvro value, String expectedType) {
        var metadata = value.getMetadata();
        assertNotNull(metadata);
        assertNotNull(metadata.getEventId());
        assertNotNull(metadata.getOccurredAt());
        assertEquals(expectedType, metadata.getEventType().toString());
        assertEquals("events-producer", metadata.getSource().toString());
        assertEquals(1, metadata.getEventVersion());
    }

    private static LibraryEventAvro withoutMetadata(LibraryEventAvro value) {
        return LibraryEventAvro.newBuilder(value).setMetadata(null).build();
    }
}
