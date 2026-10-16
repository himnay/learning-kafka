package com.learnkafka.retry;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer.SingleRecordHeader;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.List;

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

    /** Failures worth retrying later: the database unreachable or briefly overloaded. */
    private static final List<Class<? extends Throwable>> RECOVERABLE = List.of(
            RecoverableDataAccessException.class,
            TransientDataAccessException.class,          // query timeout, lock contention
            DataAccessResourceFailureException.class,    // incl. CannotGetJdbcConnectionException
            CannotCreateTransactionException.class,      // JPA / JDBC transaction could not start
            SQLTransientException.class,                 // incl. Hikari's SQLTransientConnectionException
            SQLRecoverableException.class);

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
        // the drainer forwards retry-topic records to the retry topic itself: never skip that publish
        // (by default a same-topic send is silently skipped for "fatal" causes, and the offset would still commit)
        this.retryRecoverer.setSkipSameTopicFatalExceptions(false);
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
            return Math.max(0, Integer.parseInt(new String(header.value(), UTF_8).trim()));   // negative = malformed
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

    /**
     * True if any exception in the cause chain says "try again later" (listener exceptions arrive
     * wrapped). A real PostgreSQL outage never surfaces as RecoverableDataAccessException: the JPA
     * transaction fails to start (CannotCreateTransactionException), a plain JDBC call gets no connection
     * (CannotGetJdbcConnectionException, a DataAccessResourceFailureException), and underneath sits
     * Hikari's SQLTransientConnectionException.
     */
    public static boolean isRecoverable(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause() == t ? null : t.getCause()) {
            for (Class<? extends Throwable> type : RECOVERABLE) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }

    public Route route(ConsumerRecord<?, ?> record, Exception ex) {
        if (!isRecoverable(ex)) {
            return Route.DLT;
        }
        // compared before adding 1, so a huge header can't overflow into "more attempts left"
        int attemptsSoFar = properties.topic().equals(record.topic()) ? attemptOf(record) : 0;
        return attemptsSoFar < properties.maxAttempts() ? Route.RETRY : Route.DLT;
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
