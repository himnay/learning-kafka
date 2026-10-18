package com.learnkafka.retry;

import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.event.EventMetadata;
import com.learnkafka.event.EventMetadataMdcInterceptor;
import com.learnkafka.retry.RetryRouter.Route;
import com.learnkafka.retry.model.DrainResult;
import com.learnkafka.service.LibraryEventsService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Retries what waits on the retry topic, on a schedule: nothing is held in memory between attempts,
 * so a crash or restart loses no waiting time and no records.
 *
 * One run, every app.retry.interval (fixed delay, so runs never overlap):
 *  1. a fresh consumer in its own group (app.retry.group-id) joins and gets its share of the partitions;
 *  2. it notes where every partition ends NOW and processes only records below that point that have
 *     waited app.retry.delay on the retry topic (record timestamp = forward time). A record re-queued
 *     during the run, or forwarded too recently — e.g. by the other consumer app, whose schedule runs
 *     independently — waits for a later run, so attempts are at least app.retry.delay apart;
 *  3. each record runs in a Kafka transaction with its offset. If processing fails, that transaction
 *     aborts (dropping a state record it may have sent) and a second one forwards the record through
 *     RetryRouter (retry topic with retry-attempt + 1, or the DLT) together with the offset.
 * A failure of Kafka itself ends the run; nothing is committed for the current record, so the next run
 * reads it again. Records whose database work already committed are skipped by processed_event.
 */
@Slf4j
@Component
public class RetryTopicDrainer {

    private static final LogAccessor LOG_ACCESSOR = new LogAccessor(RetryTopicDrainer.class);
    private static final Duration ASSIGNMENT_WAIT = Duration.ofSeconds(10);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final ConsumerFactory<Object, Object> consumerFactory;
    // Must be the same KafkaTemplate RetryRouter publishes with: the forward joins transaction 2 only
    // through this template's executeInTransaction producer; another instance would publish outside it.
    private final KafkaTemplate<Integer, Object> kafkaTemplate;
    private final LibraryEventsService libraryEventsService;
    private final RetryRouter router;
    private final RetryProperties properties;
    private final String clientIdPrefix;

    private volatile boolean stopping;

    public RetryTopicDrainer(ConsumerFactory<Object, Object> consumerFactory, KafkaTemplate<Integer, Object> kafkaTemplate,
                             LibraryEventsService libraryEventsService, RetryRouter router, RetryProperties properties,
                             @Value("${spring.application.name}") String applicationName) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.libraryEventsService = libraryEventsService;
        this.router = router;
        this.properties = properties;
        this.clientIdPrefix = applicationName + "-retry-drainer";
    }

    @Scheduled(fixedDelayString = "${app.retry.interval}", initialDelayString = "${app.retry.interval}")
    public void scheduledDrain() {
        drainOnce();
    }

    /** One drain run (also called directly by tests). */
    public synchronized DrainResult drainOnce() {
        var tally = new Tally();
        if (stopping) {
            return tally.result();
        }
        long started = System.nanoTime();
        try (Consumer<Object, Object> consumer = consumerFactory.createConsumer(properties.groupId(), clientIdPrefix, null, readerProperties())) {
            consumer.subscribe(List.of(properties.topic()));
            if (awaitAssignment(consumer)) {
                drain(consumer, tally);
            }
        } catch (RuntimeException e) {
            log.error("Retry drain stopped early; unprocessed records wait for the next run: {}", e.getMessage(), e);
        }
        var result = tally.result();
        long millis = Duration.ofNanos(System.nanoTime() - started).toMillis();
        if (result.processed() + result.requeued() + result.deadLettered() > 0) {
            log.info("Retry drain: processed={} requeued={} deadLettered={} in {} ms",
                    result.processed(), result.requeued(), result.deadLettered(), millis);
        } else {
            log.debug("Retry drain: nothing to do ({} ms)", millis);
        }
        return result;
    }

    @PreDestroy
    public void stop() {
        stopping = true;
    }

    /**
     * Pins the Avro reader class, as the main listener does (LibraryEventsConsumer.AVRO_READER_TYPE), and
     * fetches small batches: with the database down every record can take a connection timeout. Commit
     * and isolation settings are pinned too, whatever the shared consumer config says: auto-commit on
     * close would commit records that were fetched but never processed, and read_uncommitted would make
     * endOffsets() the high watermark instead of the last stable offset.
     */
    static Properties readerProperties() {
        var props = new Properties();
        props.setProperty("specific.avro.value.type", LibraryEventAvro.class.getName());
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    /**
     * True if a record failed to deserialize because Schema Registry could not be reached (network
     * error, or a 5xx answer): an outage, not a bad record. Each run has a fresh deserializer with an
     * empty schema cache, so a short registry blip would otherwise dead-letter every waiting record.
     */
    static boolean isRegistryUnreachable(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof IOException
                    || t instanceof RestClientException rest && rest.getStatus() >= 500) {
                return true;
            }
        }
        return false;
    }

    private boolean awaitAssignment(Consumer<Object, Object> consumer) {
        long deadline = System.nanoTime() + ASSIGNMENT_WAIT.toNanos();
        while (consumer.assignment().isEmpty() && System.nanoTime() < deadline && !stopping) {
            consumer.poll(Duration.ofMillis(200));   // anything fetched here is read again after the seeks below
        }
        return !consumer.assignment().isEmpty();
    }

    private void drain(Consumer<Object, Object> consumer, Tally tally) {
        Set<TopicPartition> assignment = consumer.assignment();
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(assignment);
        List<TopicPartition> neverCommitted = new ArrayList<>();
        for (TopicPartition tp : assignment) {
            OffsetAndMetadata offset = committed.get(tp);
            if (offset != null) {
                consumer.seek(tp, offset);
            } else {
                neverCommitted.add(tp);
            }
        }
        if (!neverCommitted.isEmpty()) {          // an empty list would rewind EVERY assigned partition
            consumer.seekToBeginning(neverCommitted);
        }

        Map<TopicPartition, Long> end = consumer.endOffsets(assignment);   // read_committed: last stable offsets
        Set<TopicPartition> active = new HashSet<>();
        for (TopicPartition tp : assignment) {
            if (consumer.position(tp) < end.get(tp)) {
                active.add(tp);
            }
        }

        long dueBefore = System.currentTimeMillis() - properties.delay().toMillis();   // forwarded before this → due
        long deadline = System.nanoTime() + properties.maxRun().toNanos();
        while (!active.isEmpty() && System.nanoTime() < deadline && !stopping) {
            for (ConsumerRecord<Object, Object> record : consumer.poll(POLL_TIMEOUT)) {
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                if (!active.contains(tp)) {
                    continue;
                }
                if (record.offset() >= end.get(tp)) {   // arrived during this run: next run's work
                    active.remove(tp);
                    continue;
                }
                if (record.timestamp() > dueBefore) {   // forwarded too recently: it and everything after it wait
                    active.remove(tp);
                    continue;
                }
                if (System.nanoTime() >= deadline) {    // max-run is checked per record, not per batch:
                    return;                             // the rest is uncommitted and read again next run
                }
                handle(consumer, record, tally);
                if (stopping) {
                    return;
                }
            }
            active.removeIf(tp -> !consumer.assignment().contains(tp) || consumer.position(tp) >= end.get(tp));
        }
    }

    private void handle(Consumer<Object, Object> consumer, ConsumerRecord<Object, Object> record, Tally tally) {
        var offsets = Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
        EventMetadataMdcInterceptor.put(EventMetadata.of(record.value()).orElse(null));
        try {
            kafkaTemplate.executeInTransaction(ops -> {
                process(record);
                ops.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                return null;
            });
            tally.processed++;
        } catch (ProcessingFailedException failed) {
            // transaction 1 has been aborted; forward the record with its offset in a new one
            RuntimeException cause = failed.getCause();
            Route route = router.route(record, cause);
            kafkaTemplate.executeInTransaction(ops -> {
                router.accept(record, consumer, cause);
                ops.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                return null;
            });
            if (route == Route.RETRY) {
                tally.requeued++;
            } else {
                tally.deadLettered++;
            }
        } finally {
            EventMetadataMdcInterceptor.clear();
        }
    }

    /** Runs the record through the same service as the main listener; any failure is the record's own. */
    @SuppressWarnings("unchecked")
    private void process(ConsumerRecord<Object, Object> record) {
        try {
            DeserializationException notAvro = SerializationUtils.getExceptionFromHeader(
                    record, KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, LOG_ACCESSOR);
            if (notAvro != null && isRegistryUnreachable(notAvro)) {
                throw new RegistryUnreachableException(notAvro);   // ends the run; nothing committed for it
            }
            if (notAvro != null) {
                throw notAvro;   // value is null; the DLT gets the original bytes from the header
            }
            libraryEventsService.processLibraryEvent((ConsumerRecord<Integer, LibraryEventAvro>) (ConsumerRecord<?, ?>) record);
        } catch (RegistryUnreachableException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ProcessingFailedException(e);
        }
    }

    /** Schema Registry could not be reached: not the record's fault, so the run stops without committing it. */
    private static final class RegistryUnreachableException extends RuntimeException {

        RegistryUnreachableException(Throwable cause) {
            super("Schema Registry unreachable; the record waits for the next run", cause);
        }
    }

    /** A failure of the record itself, as opposed to Kafka failing to commit the transaction. */
    private static final class ProcessingFailedException extends RuntimeException {

        ProcessingFailedException(RuntimeException cause) {
            super(cause);
        }

        @Override
        public synchronized RuntimeException getCause() {
            return (RuntimeException) super.getCause();
        }
    }

    private static final class Tally {
        int processed;
        int requeued;
        int deadLettered;

        DrainResult result() {
            return new DrainResult(processed, requeued, deadLettered);
        }
    }
}
