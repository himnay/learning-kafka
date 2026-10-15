package com.learnkafka.retry;

import com.learnkafka.retry.RetryRouter.Route;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.hibernate.exception.JDBCConnectionException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLTransientConnectionException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("RetryRouter: retry topic or DLT, decided by the retry-attempt header")
class RetryRouterTest {

    private static final String MAIN = "library-events";
    private static final String RETRY = "library-events.retry";
    private static final String DLT = "library-events.DLT";

    @Mock
    KafkaOperations<Object, Object> template;

    RetryRouter router;

    @BeforeEach
    void setUp() {
        var properties = new RetryProperties(RETRY, "library-events-retry-group", Duration.ofSeconds(30), 3, Duration.ofMinutes(1), Duration.ofSeconds(30));
        router = new RetryRouter(template, properties, DLT);
    }

    @Test
    @DisplayName("First recoverable failure on the main topic: retry topic, same partition, retry-attempt 1")
    void firstRecoverableFailure_goesToRetryTopicWithAttemptOne() {
        stubSend();

        router.accept(record(MAIN, null), null, new RecoverableDataAccessException("db down"));

        var sent = sent();
        assertThat(sent.topic()).isEqualTo(RETRY);
        assertThat(sent.partition()).isEqualTo(2);
        assertThat(attemptValues(sent)).containsExactly("1");
    }

    @Test
    @DisplayName("Each hop replaces retry-attempt instead of adding a second value")
    void attemptHeaderIsReplacedNotAppended() {
        stubSend();

        router.accept(record(RETRY, "2"), null, new RecoverableDataAccessException("db down"));

        var sent = sent();
        assertThat(sent.topic()).isEqualTo(RETRY);
        assertThat(attemptValues(sent)).containsExactly("3");
    }

    @Test
    @DisplayName("A record that has used all its attempts goes to the DLT and keeps its final count")
    void exhaustedAttempts_goToDlt() {
        stubSend();

        router.accept(record(RETRY, "3"), null, new RecoverableDataAccessException("db down"));

        var sent = sent();
        assertThat(sent.topic()).isEqualTo(DLT);
        assertThat(sent.partition()).isEqualTo(2);
        assertThat(attemptValues(sent)).containsExactly("3");
    }

    @Test
    @DisplayName("A non-recoverable failure goes to the DLT at once, without a retry-attempt header")
    void nonRecoverable_goesToDltAtOnce() {
        stubSend();

        router.accept(record(MAIN, null), null,
                new ListenerExecutionFailedException("listener failed", new IllegalArgumentException("bad")));

        var sent = sent();
        assertThat(sent.topic()).isEqualTo(DLT);
        assertThat(attemptValues(sent)).isEmpty();
    }

    @Test
    @DisplayName("RecoverableDataAccessException counts wherever it sits in the cause chain")
    void recoverableIsFoundAnywhereInTheCauseChain() {
        var wrapped = new ListenerExecutionFailedException("listener failed",
                new RuntimeException(new RecoverableDataAccessException("db down")));

        assertThat(router.route(record(MAIN, null), wrapped)).isEqualTo(Route.RETRY);
    }

    @Test
    @DisplayName("PostgreSQL down while the JPA transaction starts (Hikari times out) is recoverable")
    void databaseDownWhenTheTransactionStarts_isRecoverable() {
        var outage = new CannotCreateTransactionException("Could not open JPA EntityManager for transaction",
                new JDBCConnectionException("Unable to acquire JDBC Connection",
                        new SQLTransientConnectionException("HikariPool-1 - Connection is not available, request timed out after 2000ms")));

        assertThat(router.route(record(MAIN, null), outage)).isEqualTo(Route.RETRY);
    }

    @Test
    @DisplayName("No connection from the pool for a plain JDBC call is recoverable")
    void noConnectionFromThePool_isRecoverable() {
        var outage = new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available"));

        assertThat(router.route(record(MAIN, null), outage)).isEqualTo(Route.RETRY);
    }

    @Test
    @DisplayName("A transient data access failure (e.g. a query timeout) is recoverable")
    void transientDataAccessFailure_isRecoverable() {
        assertThat(router.route(record(MAIN, null), new QueryTimeoutException("statement timeout"))).isEqualTo(Route.RETRY);
    }

    @Test
    @DisplayName("A constraint violation is not recoverable: retrying can never succeed")
    void constraintViolation_isNotRecoverable() {
        assertThat(router.route(record(MAIN, null), new DataIntegrityViolationException("duplicate key")))
                .isEqualTo(Route.DLT);
    }

    @Test
    @DisplayName("A retry-attempt value that is not a number counts as no attempt yet")
    void malformedHeader_countsAsFirstHop() {
        stubSend();
        var record = record(RETRY, "abc");

        router.accept(record, null, new RecoverableDataAccessException("db down"));

        assertThat(RetryRouter.attemptOf(record)).isZero();
        assertThat(attemptValues(sent())).containsExactly("1");
    }

    @Test
    @DisplayName("A main-topic record that still carries retry-attempt (re-driven from the DLT) starts again at 1")
    void mainTopicRecordWithStaleHeader_startsAgainAtOne() {
        stubSend();

        router.accept(record(MAIN, "3"), null, new RecoverableDataAccessException("db down"));

        var sent = sent();
        assertThat(sent.topic()).isEqualTo(RETRY);
        assertThat(attemptValues(sent)).containsExactly("1");
    }

    private void stubSend() {
        when(template.send(any(ProducerRecord.class))).thenAnswer(invocation -> {
            ProducerRecord<Object, Object> out = invocation.getArgument(0);
            var metadata = new RecordMetadata(new TopicPartition(out.topic(), out.partition()), 0, 0, 0L, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(out, metadata));
        });
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<Object, Object> sent() {
        ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }

    private static ConsumerRecord<Object, Object> record(String topic, String attempt) {
        var headers = new RecordHeaders();
        if (attempt != null) {
            headers.add(RetryRouter.ATTEMPT_HEADER, attempt.getBytes(UTF_8));
        }
        return new ConsumerRecord<>(topic, 2, 42L, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, 7, "payload", headers, Optional.empty());
    }

    private static List<String> attemptValues(ProducerRecord<?, ?> record) {
        return StreamSupport.stream(record.headers().headers(RetryRouter.ATTEMPT_HEADER).spliterator(), false)
                .map(header -> new String(header.value(), UTF_8))
                .toList();
    }
}
