package com.learnkafka.retry;

import com.learnkafka.AvroEvents;
import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Recoverable failures leave library-events for library-events.retry; the scheduled drain retries
 * them. The schedule never fires here (app.retry.interval=1h): tests call the drain run directly.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(topics = {"library-events", "coffee-orders"}, partitions = 3, brokerProperties = {
        // single embedded broker: the transaction state log must fit on one node
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        // ...and so must the share-group state topic used by the barista share group
        "share.coordinator.state.topic.replication.factor=1",
        "share.coordinator.state.topic.min.isr=1"
})
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroEvents.REGISTRY,
        "app.retry.interval=1h"
})
class RetryTopicIntegrationTest {

    private static final String MAIN_TOPIC = "library-events";
    private static final String RETRY_TOPIC = "library-events.retry";
    private static final String DLT_TOPIC = "library-events.DLT";

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, Object> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Autowired
    RetryTopicDrainer drainer;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @MockitoSpyBean
    LibraryEventsService serviceSpy;

    @MockitoSpyBean
    RetryRouter routerSpy;

    @BeforeEach
    void waitForAssignment() {
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            // share-group containers have no partition assignment to wait for
            if (container instanceof ConcurrentMessageListenerContainer<?, ?>) {
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
            }
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    @DisplayName("A recoverable failure is forwarded once to library-events.retry (retry-attempt 1), not re-published to library-events")
    void recoverableFailure_isForwardedToRetryTopicOnce() {
        var event = newEvent(9101, "Retry-Once");
        doThrow(new RecoverableDataAccessException("database down"))
                .when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Once")));

        kafkaTemplate.executeInTransaction(t -> t.sendDefault(9101, event));

        var forwarded = awaitRecord(RETRY_TOPIC, r -> event.equals(AvroEvents.decode(RETRY_TOPIC, r.value())));
        assertThat(headerValues(forwarded, RetryRouter.ATTEMPT_HEADER)).containsExactly("1");
        assertThat(headerValues(forwarded, "kafka_dlt-original-topic")).containsExactly(MAIN_TOPIC);
        assertThat(readAll(MAIN_TOPIC).stream().filter(r -> isEvent(event, MAIN_TOPIC, r)).count())
                .as("records for the event on library-events (a re-publish loop would add more)")
                .isEqualTo(1);
        verify(serviceSpy, times(3)).processLibraryEvent(argThat(isBook("Retry-Once")));   // 1 + 2 in-place retries
    }

    @Test
    @DisplayName("A drain run processes the waiting record once the database is back: the book is saved, nothing is retried twice")
    void drainRun_succeeds_onceTheDatabaseRecovers() {
        var event = newEvent(9102, "Retry-Recovers");
        var down = new RecoverableDataAccessException("database down");
        doThrow(down).doThrow(down).doThrow(down).doCallRealMethod()
                .when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Recovers")));
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(9102, event));
        awaitRecord(RETRY_TOPIC, r -> isEvent(event, RETRY_TOPIC, r) && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of("1")));

        drainer.drainOnce();

        assertThat(libraryEventsRepository.findAll())
                .anySatisfy(saved -> assertThat(saved.getBook().getBookName()).isEqualTo("Retry-Recovers"));
        drainer.drainOnce();
        verify(serviceSpy, times(4)).processLibraryEvent(argThat(isBook("Retry-Recovers")));
    }

    @Test
    @DisplayName("A record that keeps failing gets one attempt per run, then the DLT with retry-attempt 3 and the exception headers")
    void alwaysFailing_isDeadLetteredAfterMaxAttempts_oneAttemptPerRun() {
        var event = newEvent(9103, "Retry-Never");
        doThrow(new RecoverableDataAccessException("database down"))
                .when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Never")));
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(9103, event));
        awaitRecord(RETRY_TOPIC, r -> isEvent(event, RETRY_TOPIC, r) && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of("1")));

        for (String next : List.of("2", "3")) {
            long before = callsFor("Retry-Never");
            drainer.drainOnce();
            assertThat(callsFor("Retry-Never")).as("attempts in one run").isEqualTo(before + 1);
            awaitRecord(RETRY_TOPIC, r -> isEvent(event, RETRY_TOPIC, r) && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of(next)));
        }
        long before = callsFor("Retry-Never");
        drainer.drainOnce();
        assertThat(callsFor("Retry-Never")).isEqualTo(before + 1);

        var dead = awaitRecord(DLT_TOPIC, r -> isEvent(event, DLT_TOPIC, r));
        assertThat(headerValues(dead, RetryRouter.ATTEMPT_HEADER)).containsExactly("3");
        assertThat(headerValues(dead, "kafka_dlt-exception-fqcn")).containsExactly(RecoverableDataAccessException.class.getName());
        assertThat(headerValues(dead, "kafka_dlt-original-topic")).containsExactly(MAIN_TOPIC);
    }

    @Test
    @DisplayName("Bytes on the retry topic that are not Avro go to the DLT unchanged instead of breaking the run")
    void undeserializableRecordOnRetryTopic_isDeadLetteredWithOriginalBytes() throws Exception {
        byte[] garbage = "not avro at all".getBytes(UTF_8);
        try (var producer = new DefaultKafkaProducerFactory<byte[], byte[]>(KafkaTestUtils.producerProps(embeddedKafkaBroker),
                new ByteArraySerializer(), new ByteArraySerializer()).createProducer()) {
            var record = new ProducerRecord<byte[], byte[]>(RETRY_TOPIC, 1, null, garbage);
            record.headers().add(RetryRouter.ATTEMPT_HEADER, "1".getBytes(UTF_8));
            producer.send(record).get();
        }

        drainer.drainOnce();

        assertThat(awaitRecord(DLT_TOPIC, r -> Arrays.equals(r.value(), garbage))).isNotNull();
    }

    @Test
    @DisplayName("If forwarding fails, nothing is committed for the record: the next run reads it again")
    void failedForward_leavesTheRecordForTheNextRun() {
        var event = newEvent(9104, "Retry-Forward-Fails");
        doThrow(new RecoverableDataAccessException("database down"))
                .when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Forward-Fails")));
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(9104, event));
        awaitRecord(RETRY_TOPIC, r -> isEvent(event, RETRY_TOPIC, r) && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of("1")));
        doThrow(new KafkaException("broker down")).doCallRealMethod()
                .when(routerSpy).accept(argThat(recordOf("Retry-Forward-Fails")), any(), any());

        long before = callsFor("Retry-Forward-Fails");
        drainer.drainOnce();
        assertThat(callsFor("Retry-Forward-Fails")).isEqualTo(before + 1);
        assertThat(readAll(RETRY_TOPIC)).noneMatch(r -> isEvent(event, RETRY_TOPIC, r)
                && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of("2")));

        drainer.drainOnce();
        assertThat(callsFor("Retry-Forward-Fails")).isEqualTo(before + 2);
        awaitRecord(RETRY_TOPIC, r -> isEvent(event, RETRY_TOPIC, r) && headerValues(r, RetryRouter.ATTEMPT_HEADER).equals(List.of("2")));
    }

    private long callsFor(String bookName) {
        var matcher = isBook(bookName);
        return mockingDetails(serviceSpy).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("processLibraryEvent"))
                .filter(invocation -> matcher.matches(invocation.getArgument(0)))
                .count();
    }

    private static ArgumentMatcher<ConsumerRecord<?, ?>> recordOf(String bookName) {
        return r -> r != null && r.value() instanceof LibraryEventAvro event
                && bookName.equals(event.getBook().getBookName().toString());
    }

    private static LibraryEventAvro newEvent(int bookId, String bookName) {
        return AvroEvents.fromJson("{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":" + bookId
                + ",\"bookName\":\"" + bookName + "\",\"bookAuthor\":\"Retry\"}}");
    }

    private static ArgumentMatcher<ConsumerRecord<Integer, LibraryEventAvro>> isBook(String bookName) {
        return r -> r != null && r.value() != null && bookName.equals(r.value().getBook().getBookName().toString());
    }

    /** True if the raw record decodes to the event; anything that isn't this event's Avro is just "no". */
    private static boolean isEvent(LibraryEventAvro event, String topic, ConsumerRecord<byte[], byte[]> record) {
        try {
            return record.value() != null && event.equals(AvroEvents.decode(topic, record.value()));
        } catch (RuntimeException notThisEvent) {
            return false;
        }
    }

    private static List<String> headerValues(ConsumerRecord<?, ?> record, String name) {
        return StreamSupport.stream(record.headers().headers(name).spliterator(), false)
                .map(Header::value)
                .map(value -> new String(value, UTF_8))
                .toList();
    }

    /** Reads the topic from the beginning until a record matches (15 s at most). */
    private ConsumerRecord<byte[], byte[]> awaitRecord(String topic, Predicate<ConsumerRecord<byte[], byte[]>> matches) {
        try (Consumer<byte[], byte[]> reader = newReader()) {
            reader.subscribe(List.of(topic));
            var found = new AtomicReference<ConsumerRecord<byte[], byte[]>>();
            Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> {
                for (ConsumerRecord<byte[], byte[]> r : reader.poll(Duration.ofMillis(500))) {
                    if (found.get() == null && matchesSafely(matches, r)) {
                        found.set(r);
                    }
                }
                return found.get() != null;
            });
            return found.get();
        }
    }

    /** Every record currently on the topic: reads from the beginning until two polls come back empty. */
    private List<ConsumerRecord<byte[], byte[]>> readAll(String topic) {
        try (Consumer<byte[], byte[]> reader = newReader()) {
            reader.subscribe(List.of(topic));
            var all = new ArrayList<ConsumerRecord<byte[], byte[]>>();
            int emptyPolls = 0;
            while (emptyPolls < 2 || reader.assignment().isEmpty()) {
                var records = reader.poll(Duration.ofMillis(500));
                records.forEach(all::add);
                emptyPolls = records.isEmpty() ? emptyPolls + 1 : 0;
            }
            return all;
        }
    }

    private Consumer<byte[], byte[]> newReader() {
        var props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "retry-test-reader-" + System.nanoTime(), false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return new DefaultKafkaConsumerFactory<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer()).createConsumer();
    }

    private static boolean matchesSafely(Predicate<ConsumerRecord<byte[], byte[]>> matches, ConsumerRecord<byte[], byte[]> record) {
        try {
            return matches.test(record);
        } catch (RuntimeException notThisRecord) {
            return false;
        }
    }
}
