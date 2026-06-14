package com.learnkafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventProducer {

    public static final String TOPIC = "library-events";

    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Fire-and-forget send using the default topic (async, CompletableFuture callback).
     * Strategy Pattern: delegates completion handling to handleSuccess / handleFailure.
     */
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent)
            throws JsonProcessingException {
        var key   = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        return kafkaTemplate.sendDefault(key, value)
                .whenComplete((result, ex) -> {
                    if (ex != null) handleFailure(key, value, ex);
                    else            handleSuccess(key, value, result);
                });
    }

    /**
     * Send via explicit ProducerRecord with custom headers (async).
     * Builder Pattern: ProducerRecord constructed via buildProducerRecord factory method.
     */
    public CompletableFuture<SendResult<Integer, String>> sendLibraryEventWithHeaders(LibraryEvent libraryEvent)
            throws JsonProcessingException {
        var key            = libraryEvent.libraryEventId();
        var value          = objectMapper.writeValueAsString(libraryEvent);
        var producerRecord = buildProducerRecord(key, value, TOPIC);
        return kafkaTemplate.send(producerRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) handleFailure(key, value, ex);
                    else            handleSuccess(key, value, result);
                });
    }

    /**
     * Synchronous send — blocks up to 1 second. Use only for PUT (idempotent update) flows
     * where the caller needs confirmation before returning a response.
     */
    public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
            throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        var key   = libraryEvent.libraryEventId();
        var value = objectMapper.writeValueAsString(libraryEvent);
        return kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {
        List<Header> headers = List.of(new RecordHeader("event-source", "scanner".getBytes()));
        return new ProducerRecord<>(topic, null, key, value, headers);
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Failed to send message [key={}] : {}", key, ex.getMessage(), ex);
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Sent [key={}, partition={}] value={}", key, result.getRecordMetadata().partition(), value);
    }
}
