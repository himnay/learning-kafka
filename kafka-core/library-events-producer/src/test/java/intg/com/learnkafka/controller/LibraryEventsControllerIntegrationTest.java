package com.learnkafka.controller;

import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false"
})
class LibraryEventsControllerIntegrationTest {

    @LocalServerPort
    int port;

    RestClient restClient;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        restClient = RestClient.builder()
                .baseUrl("http://localhost:" + port)
                .build();
        var configs = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(configs, new IntegerDeserializer(), new StringDeserializer())
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
        assertEquals(
                "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Dilip\"}}",
                record.value());
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
        assertEquals(
                "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Dilip\"}}",
                record.value());
    }
}
