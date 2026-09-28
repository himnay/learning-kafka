package com.learnkafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Learning example: demonstrates manual offset commit (MANUAL_IMMEDIATE ack mode).
 * Activated only under the "manual-offset" Spring profile, which also switches the default
 * {@link LibraryEventsConsumer} off; it listens through the manual-ack container factory, so the
 * offset is committed only when {@code acknowledge()} is called.
 */
@Slf4j
@Component
@Profile("manual-offset")
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener<Integer, String> {

    @Override
    @KafkaListener(topics = {"library-events"}, groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = LibraryEventsConsumerConfig.MANUAL_ACK_CONTAINER_FACTORY)
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("ConsumerRecord (manual offset): {}", consumerRecord);
        acknowledgment.acknowledge();
    }
}
