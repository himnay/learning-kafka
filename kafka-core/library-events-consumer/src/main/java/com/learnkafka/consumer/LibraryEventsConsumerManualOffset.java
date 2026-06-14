package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Learning example: demonstrates manual offset commit (MANUAL_IMMEDIATE ack mode).
 * Activated only under the "manual-offset" Spring profile so it never conflicts
 * with the default auto-commit consumer in other profiles.
 */
@Component
@Profile("manual-offset")
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord (manual offset): {}", consumerRecord);
        acknowledgment.acknowledge();
    }
}
