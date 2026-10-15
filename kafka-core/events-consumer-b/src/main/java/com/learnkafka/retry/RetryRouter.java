package com.learnkafka.retry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.SingleRecordHeader;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * One place for the retry rule, used by the main listener's AfterRollbackProcessor and by the
 * scheduled RetryTopicDrainer: a recoverable failure goes to the retry topic with its
 * {@code retry-attempt} header counted up, everything else (and a record out of attempts) to the DLT.
 *
 * Both destinations are DeadLetterPublishingRecoverers, so the record keeps its partition number and
 * its producer headers, and gets Spring's diagnostics: {@code kafka_dlt-exception-*} for the latest
 * failure, {@code kafka_dlt-original-*} for its position on library-events (first hop only).
 */
@Slf4j
public class RetryRouter implements ConsumerAwareRecordRecoverer {

    /** Times the record has been forwarded to the retry topic: "1" on the first hop. */
    public static final String ATTEMPT_HEADER = "retry-attempt";

    public enum Route { RETRY, DLT }

    private final RetryProperties properties;
    private final DeadLetterPublishingRecoverer retryRecoverer;
    private final DeadLetterPublishingRecoverer dltRecoverer;

    public RetryRouter(KafkaOperations<?, ?> template, RetryProperties properties, String dltTopic) {
        this.properties = properties;
        this.retryRecoverer = new DeadLetterPublishingRecoverer(template, (record, ex) -> {
            log.warn("Forwarding to {} attempt={}: key={} cause={}", properties.topic(), nextAttempt(record),
                    record.key(), ex.getMessage());
            return new TopicPartition(properties.topic(), record.partition());
        });
        // SingleRecordHeader REPLACES an existing retry-attempt instead of adding a second value
        this.retryRecoverer.setHeadersFunction((record, ex) -> new RecordHeaders(new Header[]{
                new SingleRecordHeader(ATTEMPT_HEADER, String.valueOf(nextAttempt(record)).getBytes(UTF_8))}));
        this.retryRecoverer.setAppendOriginalHeaders(false);   // keep the library-events position, not the retry topic's
        this.dltRecoverer = new DeadLetterPublishingRecoverer(template, (record, ex) -> {
            log.error("Routing to DLT: key={} cause={}", record.key(), ex.getMessage());
            return new TopicPartition(dltTopic, record.partition());
        });
        this.dltRecoverer.setAppendOriginalHeaders(false);
    }

    /** The record's retry-attempt header; 0 when it has none or it isn't a number. */
    public static int attemptOf(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(ATTEMPT_HEADER);
        if (header == null) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(header.value(), UTF_8).trim());
        } catch (NumberFormatException notANumber) {
            return 0;
        }
    }

    /**
     * The attempt number this record would carry on the retry topic. Only a record read from the retry
     * topic counts its header: one on library-events (e.g. re-driven from the DLT) starts again at 1.
     */
    public int nextAttempt(ConsumerRecord<?, ?> record) {
        return properties.topic().equals(record.topic()) ? attemptOf(record) + 1 : 1;
    }

    /** RecoverableDataAccessException anywhere in the cause chain (listener exceptions arrive wrapped). */
    public static boolean isRecoverable(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof RecoverableDataAccessException) {
                return true;
            }
        }
        return false;
    }

    public Route route(ConsumerRecord<?, ?> record, Exception ex) {
        return isRecoverable(ex) && nextAttempt(record) <= properties.maxAttempts() ? Route.RETRY : Route.DLT;
    }

    /** Publishes the record to the retry topic or the DLT; {@code consumer} may be null. */
    @Override
    public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception ex) {
        if (route(record, ex) == Route.RETRY) {
            retryRecoverer.accept(record, consumer, ex);
        } else {
            dltRecoverer.accept(record, consumer, ex);
        }
    }
}
