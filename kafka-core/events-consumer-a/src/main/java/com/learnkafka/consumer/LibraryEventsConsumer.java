package com.learnkafka.consumer;

import com.learnkafka.avro.LibraryEventAvro;

import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Observer Pattern: Spring Kafka registers this listener as an observer on the "library-events" topic.
 * Each partition message triggers onMessage(), which delegates processing to LibraryEventsService.
 * Replaced by {@link LibraryEventsConsumerManualOffset} under the "manual-offset" profile.
 *
 * Extends AbstractConsumerSeekAware so the app can REPLAY: Spring Kafka hands it a seek callback
 * per assigned partition, and ReplayController asks it to rewind a partition it currently owns.
 */
@Slf4j
@Component
@RequiredArgsConstructor
@Profile("!manual-offset")
public class LibraryEventsConsumer extends AbstractConsumerSeekAware {

    /**
     * Pin the Avro reader class. Without it, KafkaAvroDeserializer picks the class from the
     * WRITER's schema name — so records written before LibraryEvent was renamed to
     * LibraryEventAvro would fail ("Could not find class ...LibraryEvent"). Pinned, Avro
     * resolves old records through the schema's aliases.
     */
    public static final String AVRO_READER_TYPE = "specific.avro.value.type=com.learnkafka.avro.LibraryEventAvro";

    public static final String TOPIC = "library-events";

    private final LibraryEventsService libraryEventsService;

    /** Handles message. */
    @KafkaListener(topics = TOPIC, groupId = "${spring.kafka.consumer.group-id}",
            properties = AVRO_READER_TYPE)
    public void onMessage(ConsumerRecord<Integer, LibraryEventAvro> consumerRecord) {
        log.info("Received ConsumerRecord : {}", consumerRecord);
        libraryEventsService.processLibraryEvent(consumerRecord);
    }

    /** Partitions of library-events this app owns right now (the rest belong to the other app). */
    public Set<Integer> assignedPartitions() {
        return getTopicsAndCallbacks().keySet().stream()
                .filter(tp -> TOPIC.equals(tp.topic()))
                .map(TopicPartition::partition)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * Rewinds one partition. The seek runs on the consumer thread before its next poll().
     *
     * @return false if this app doesn't own the partition (ask the other consumer app)
     */
    public boolean replay(int partition, long offset) {
        var callbacks = getSeekCallbacksFor(new TopicPartition(TOPIC, partition));
        if (callbacks == null || callbacks.isEmpty()) {
            return false;
        }
        callbacks.forEach(callback -> callback.seek(TOPIC, partition, offset));
        log.warn("Replay requested: {}-{} from offset {}", TOPIC, partition, offset);
        return true;
    }

    /** Rewinds every owned partition to the first offset at/after the timestamp (epoch ms). */
    public Set<Integer> replayFrom(long timestamp) {
        var owned = assignedPartitions();
        seekToTimestamp(timestamp);
        log.warn("Replay requested: {} partitions {} from timestamp {}", TOPIC, owned, timestamp);
        return owned;
    }
}
