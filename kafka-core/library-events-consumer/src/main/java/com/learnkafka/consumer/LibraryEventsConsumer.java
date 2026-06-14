package com.learnkafka.consumer;

import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Observer Pattern: Spring Kafka registers this listener as an observer on the "library-events" topic.
 * Each partition message triggers onMessage(), which delegates processing to LibraryEventsService.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumer {

    private final LibraryEventsService libraryEventsService;

    @KafkaListener(topics = {"library-events"}, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
        log.info("Received ConsumerRecord : {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }
}
