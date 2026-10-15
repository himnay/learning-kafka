package com.learnkafka.service;

import com.learnkafka.avro.LibraryEventAvro;

import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.event.ReceivedEvent;
import com.learnkafka.idempotency.ProcessedEventRepository;
import com.learnkafka.mapper.LibraryEventEntityMapper;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * GoF Template Method: processLibraryEvent() defines the algorithm skeleton;
 * concrete steps (save, validate) are dispatch-selected by event type.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LibraryEventsService {

    /** Compacted topic: latest LibraryEventAvro per libraryEventId (see LibraryEventsConsumerConfig). */
    public static final String STATE_TOPIC = "library-book-state";

    private final KafkaTemplate<Integer, Object> kafkaTemplate;
    private final LibraryEventsRepository libraryEventsRepository;
    private final ProcessedEventRepository processedEventRepository;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroup;

    /**
     * Processes library event in ONE database transaction (JPA transaction manager):
     * the idempotency marker and the business rows commit or roll back together.
     * The Kafka transaction (offset commit) is started by the listener container and
     * commits after this method returns.
     */
    @Transactional("transactionManager")
    public void processLibraryEvent(ConsumerRecord<Integer, LibraryEventAvro> consumerRecord) {
        if (!processedEventRepository.markProcessed(consumerGroup, consumerRecord)) {
            // Redelivery (e.g. DB committed but the Kafka offset commit failed) — already applied
            log.info("Skipping duplicate record topic={} partition={} offset={} eventId={}",
                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                    ReceivedEvent.of(consumerRecord.value()).eventId());
            return;
        }
        // The value is already a typed Avro record (KafkaAvroDeserializer + Schema Registry);
        // bytes that are not valid Avro never reach here — they fail in the deserializer.
        var received = ReceivedEvent.of(consumerRecord.value());   // child event + common metadata
        LibraryEvent libraryEvent = LibraryEventEntityMapper.toEntity(received.payload());
        log.info("Processing {} eventId={} occurredAt={} : {}", received.eventType(), received.eventId(),
                received.occurredAt(), libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW    -> { validateNew(libraryEvent); save(libraryEvent); }
            case UPDATE -> { validate(libraryEvent); save(libraryEvent); }
            default     -> log.warn("Unsupported LibraryEventType: {}", libraryEvent.getLibraryEventType());
        }
    }

    /**
     * A NEW event must not carry an id: the database assigns it. With an id, save() would merge it
     * as an existing row and Hibernate (6.6+) would fail with an optimistic-lock error on every retry.
     * Not retryable — it can never succeed — so it goes straight to the DLT.
     */
    private void validateNew(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() != null) {
            throw new IllegalArgumentException("NEW library event must not have an id, got " + libraryEvent.getLibraryEventId());
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
        var saved = libraryEventsRepository.save(libraryEvent);
        log.info("Persisted libraryEvent {}", saved);
        publishLatestState(saved);
    }

    /**
     * Consume-process-PRODUCE: the new state goes to the compacted topic in the SAME Kafka
     * transaction as the consumed offset (the listener container's), so it is published exactly
     * when the record counts as processed. Called outside a listener (e.g. a test calling the
     * service directly), the send joins the database transaction's synchronization instead.
     */
    private void publishLatestState(LibraryEvent saved) {
        kafkaTemplate.send(STATE_TOPIC, saved.getLibraryEventId(), LibraryEventEntityMapper.toAvro(saved));
    }
}
