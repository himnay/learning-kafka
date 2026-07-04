package com.learnkafka.producer;

import tools.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerUnitTest {

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    LibraryEventProducer eventProducer;

    private Book buildBook() {
        return Book.builder().bookId(123).bookAuthor("Dilip").bookName("Kafka using Spring Boot").build();
    }

    private LibraryEvent buildEvent() {
        return LibraryEvent.builder().libraryEventId(null).book(buildBook()).build();
    }

    @Test
    @DisplayName("Producer send failure propagates the underlying Kafka exception to the caller")
    void sendLibraryEventWithHeaders_failure() throws ExecutionException, InterruptedException {
        CompletableFuture<SendResult<Integer, String>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Exception Calling Kafka"));
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> eventProducer.sendLibraryEventWithHeaders(buildEvent()).get());
    }

    @Test
    @DisplayName("Producer successfully sends the library event and returns the resulting record metadata")
    void sendLibraryEventWithHeaders_success() throws ExecutionException, InterruptedException {
        var libraryEvent = buildEvent();
        var record = objectMapper.writeValueAsString(libraryEvent);

        var producerRecord = new ProducerRecord<Integer, String>("library-events", libraryEvent.libraryEventId(), record);
        var recordMetadata  = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, System.currentTimeMillis(), 1, 2);
        var sendResult       = new SendResult<>(producerRecord, recordMetadata);

        CompletableFuture<SendResult<Integer, String>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        var result = eventProducer.sendLibraryEventWithHeaders(libraryEvent).get();

        assertEquals(1, result.getRecordMetadata().partition());
    }
}
