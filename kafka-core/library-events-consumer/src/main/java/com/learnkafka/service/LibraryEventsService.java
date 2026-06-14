package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * GoF Template Method: processLibraryEvent() defines the algorithm skeleton;
 * concrete steps (save, validate) are dispatch-selected by event type.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<Integer, String> kafkaTemplate;
    private final LibraryEventsRepository libraryEventsRepository;

    @Transactional
    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) {
        LibraryEvent libraryEvent;
        try {
            libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize library event: " + e.getMessage(), e);
        }
        log.info("Processing libraryEvent : {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW    -> save(libraryEvent);
            case UPDATE -> { validate(libraryEvent); save(libraryEvent); }
            default     -> log.warn("Unsupported LibraryEventType: {}", libraryEvent.getLibraryEventType());
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library Event Id is missing");
        }
        libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
                .orElseThrow(() -> new IllegalArgumentException("Not a valid library Event: " + libraryEvent.getLibraryEventId()));
        log.info("Validation successful for libraryEvent : {}", libraryEvent.getLibraryEventId());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Persisted libraryEvent {}", libraryEvent);
    }

    public CompletableFuture<SendResult<Integer, String>> handleRecovery(
            ConsumerRecord<Integer, String> record) {
        return kafkaTemplate.sendDefault(record.key(), record.value())
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Recovery re-publish failed for key={} : {}", record.key(), ex.getMessage(), ex);
                    } else {
                        log.info("Recovery re-published key={} to partition={}", record.key(),
                                result.getRecordMetadata().partition());
                    }
                });
    }
}
