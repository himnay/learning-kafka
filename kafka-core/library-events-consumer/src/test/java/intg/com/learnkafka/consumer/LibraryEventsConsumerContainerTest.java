package com.learnkafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Integration tests using a real Confluent Kafka container (TestContainers).
 * Uses the exact same image as the docker-compose.yml (confluentinc/cp-kafka:7.7.1)
 * to ensure parity with the local dev environment.
 */
@SpringBootTest
@Testcontainers
class LibraryEventsConsumerContainerTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.producer.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.template.default-topic", () -> "library-events");
        registry.add("spring.kafka.producer.properties.enable.idempotence", () -> "false");
    }

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @MockitoSpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, 3);
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    void publishNewLibraryEvent_persistsToDatabase() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = """
                {"libraryEventId":null,"libraryEventType":"NEW",
                 "book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}
                """;
        kafkaTemplate.sendDefault(json).get();

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
                    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

                    List<LibraryEvent> saved = libraryEventsRepository.findAll();
                    assertEquals(1, saved.size());
                    assertEquals(456, saved.get(0).getBook().getBookId());
                });
    }

    @Test
    void publishUpdateLibraryEvent_updatesDatabase() throws JsonProcessingException, ExecutionException, InterruptedException {
        String newJson = """
                {"libraryEventId":null,"libraryEventType":"NEW",
                 "book":{"bookId":456,"bookName":"Kafka Using Spring Boot","bookAuthor":"Dilip"}}
                """;
        LibraryEvent libraryEvent = objectMapper.readValue(newJson, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(456).bookName("Kafka Using Spring Boot 3.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
                    LibraryEvent persisted = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).orElseThrow();
                    assertEquals("Kafka Using Spring Boot 3.x", persisted.getBook().getBookName());
                });
    }

    @Test
    void publishUpdateLibraryEvent_withInvalidId_isDiscarded() throws JsonProcessingException, ExecutionException, InterruptedException {
        Integer unknownId = 99999;
        String json = """
                {"libraryEventId":99999,"libraryEventType":"UPDATE",
                 "book":{"bookId":456,"bookName":"Does Not Exist","bookAuthor":"Dilip"}}
                """;
        kafkaTemplate.sendDefault(unknownId, json).get();

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
                    Optional<LibraryEvent> result = libraryEventsRepository.findById(unknownId);
                    assertFalse(result.isPresent());
                });
    }

    @Test
    void publishNewLibraryEvent_multipleEvents_allPersisted() throws JsonProcessingException, ExecutionException, InterruptedException {
        var json1 = """
                {"libraryEventId":null,"libraryEventType":"NEW",
                 "book":{"bookId":1,"bookName":"Book One","bookAuthor":"Author A"}}
                """;
        var json2 = """
                {"libraryEventId":null,"libraryEventType":"NEW",
                 "book":{"bookId":2,"bookName":"Book Two","bookAuthor":"Author B"}}
                """;

        kafkaTemplate.sendDefault(json1).get();
        kafkaTemplate.sendDefault(json2).get();

        Awaitility.await()
                .atMost(15, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<LibraryEvent> saved = libraryEventsRepository.findAll();
                    assertEquals(2, saved.size());
                    assertTrue(saved.stream().anyMatch(e -> e.getBook().getBookId() == 1));
                    assertTrue(saved.stream().anyMatch(e -> e.getBook().getBookId() == 2));
                });
    }
}
