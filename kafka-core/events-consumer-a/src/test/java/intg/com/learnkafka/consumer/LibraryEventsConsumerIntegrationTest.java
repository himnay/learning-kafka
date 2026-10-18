package com.learnkafka.consumer;

import com.learnkafka.avro.LibraryEventAvro;

import tools.jackson.databind.ObjectMapper;
import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.learnkafka.AvroEvents;
import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.coffee.CoffeeOrdersConsumer;
import com.learnkafka.event.EventMetadata;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.coffee.BaristaShareConsumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroEvents.REGISTRY
})
@EmbeddedKafka(topics = {"library-events", "coffee-orders"}, partitions = 3, brokerProperties = {
        // single embedded broker: the transaction state log must fit on one node
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        // ...and so must the share-group state topic (__share_group_state) used by the barista share group
        "share.coordinator.state.topic.replication.factor=1",
        "share.coordinator.state.topic.min.isr=1"
})
class LibraryEventsConsumerIntegrationTest {

    private static final String DLT = "library-events.DLT";

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, Object> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @MockitoSpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @MockitoSpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @MockitoSpyBean
    CoffeeOrdersConsumer coffeeOrdersConsumerSpy;

    @MockitoSpyBean
    BaristaShareConsumer baristaSpy;

    @Autowired
    ProducerFactory<Object, Object> producerFactory;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            // share-group containers have no partition assignment to wait for
            if (container instanceof ConcurrentMessageListenerContainer<?, ?>) {
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
            }
        }
        // a new share group starts at the LATEST offset; make the baristas see records sent before they joined
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString()))) {
            admin.incrementalAlterConfigs(Map.of(
                    new ConfigResource(ConfigResource.Type.GROUP, BaristaShareConsumer.SHARE_GROUP_ID),
                    List.of(new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET))))
                    .all().get();
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();
    }

    @Test
    @DisplayName("Publishing a new library event invokes the consumer and service and persists it")
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(AvroEvents.fromJson(json))).get();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
                    verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

                    List<LibraryEvent> saved = libraryEventsRepository.findAll();
                    assertEquals(1, saved.size());
                    assertEquals(456, saved.get(0).getBook().getBookId());
                });
    }

    @Test
    @DisplayName("Publishing an update library event invokes the consumer and updates the persisted book details")
    void publishUpdateLibraryEvent() throws ExecutionException, InterruptedException {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder().bookId(456).bookName("Kafka Using Spring Boot 2.x").bookAuthor("Dilip").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(libraryEvent.getLibraryEventId(), AvroEvents.fromJson(updatedJson))).get();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
                    verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
                    LibraryEvent persisted = libraryEventsRepository.findById(libraryEvent.getLibraryEventId()).get();
                    assertEquals("Kafka Using Spring Boot 2.x", persisted.getBook().getBookName());
                });
    }

    @Test
    @DisplayName("Update event with a library event id not found in the database is consumed but not persisted")
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws InterruptedException, ExecutionException {
        Integer libraryEventId = 123;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(libraryEventId, AvroEvents.fromJson(json))).get();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
                    verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
                    Optional<LibraryEvent> result = libraryEventsRepository.findById(libraryEventId);
                    assertFalse(result.isPresent());
                });
    }

    @Test
    @DisplayName("Update event with a null library event id is still consumed by the listener")
    void publishModifyLibraryEvent_Null_LibraryEventId() throws InterruptedException, ExecutionException {
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(libraryEventId, AvroEvents.fromJson(json))).get();

        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
                    verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
                });
    }

    @Test
    @DisplayName("Update for an unknown library event id is not retried and lands on the dead-letter topic")
    void invalidUpdate_isDeadLettered_withoutRetry() {
        String json = "{\"libraryEventId\":4242,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":7,\"bookName\":\"Ghost\",\"bookAuthor\":\"Nobody\"}}";
        var event = AvroEvents.fromJson(json);
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(4242, event));

        // The DLT copy is Avro again (re-serialized by the app's KafkaAvroSerializer)
        ConsumerRecord<Integer, byte[]> dead = awaitDeadLetter(bytes -> event.equals(AvroEvents.decode(DLT, bytes)));
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(), StandardCharsets.UTF_8))
                .isEqualTo(IllegalArgumentException.class.getName());
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(argThat(r -> event.equals(r.value())));
    }

    @Test
    @DisplayName("A record that is not valid Avro is not retried and lands on the dead-letter topic with its original bytes")
    void notAvro_isDeadLettered_withoutRetry() {
        byte[] garbage = ("not-avro-" + System.nanoTime()).getBytes(StandardCharsets.UTF_8);
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(garbage));   // byte[] → ByteArraySerializer

        // ErrorHandlingDeserializer turns the bad bytes into a DeserializationException:
        // the listener never runs, and the DLT gets the raw bytes back unchanged
        ConsumerRecord<Integer, byte[]> dead = awaitDeadLetter(bytes -> Arrays.equals(garbage, bytes));
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(), StandardCharsets.UTF_8))
                .isEqualTo(DeserializationException.class.getName());
        verify(libraryEventsServiceSpy, never()).processLibraryEvent(any());
    }

    @Test
    @DisplayName("A NEW event that carries an id is not retried and lands on the dead-letter topic")
    void newEventWithId_isDeadLettered_withoutRetry() {
        String json = "{\"libraryEventId\":777,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":777,\"bookName\":\"Has Id\",\"bookAuthor\":\"Nobody\"}}";
        var event = AvroEvents.fromJson(json);
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(777, event));

        ConsumerRecord<Integer, byte[]> dead = awaitDeadLetter(bytes -> event.equals(AvroEvents.decode(DLT, bytes)));
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(), StandardCharsets.UTF_8))
                .isEqualTo(IllegalArgumentException.class.getName());
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(argThat(r -> event.equals(r.value())));
        assertThat(libraryEventsRepository.findAll()).noneMatch(e -> e.getBook().getBookId() == 777);
    }

    @Test
    @DisplayName("A NEW event for a book_id that already exists violates the primary key: not retried, dead-lettered")
    void duplicateBookId_isDeadLettered_withoutRetry() {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":950,\"bookName\":\"First\",\"bookAuthor\":\"Dup\"}}";
        var first = AvroEvents.fromJson(json);
        var duplicate = AvroEvents.fromJson(json.replace("First", "Second"));
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(first));
        Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
                libraryEventsRepository.findAll().stream().anyMatch(e -> e.getBook().getBookId() == 950));

        kafkaTemplate.executeInTransaction(t -> t.sendDefault(duplicate));

        ConsumerRecord<Integer, byte[]> dead = awaitDeadLetter(bytes -> duplicate.equals(AvroEvents.decode(DLT, bytes)));
        assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-cause-fqcn").value(), StandardCharsets.UTF_8))
                .isEqualTo(DataIntegrityViolationException.class.getName());
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(argThat(r -> duplicate.equals(r.value())));
        assertThat(libraryEventsRepository.findAll()).filteredOn(e -> e.getBook().getBookId() == 950)
                .singleElement().satisfies(e -> assertThat(e.getBook().getBookName()).isEqualTo("First"));
    }

    @Test
    @DisplayName("Idempotent consumer: the same EVENT re-sent at a new offset is applied once (dedupe by eventId)")
    void resentEvent_sameEventId_isAppliedOnce() {
        var metadata = EventMetadata.newEvent("LibraryEvent.NEW", "test-producer", "req-960");
        var event = AvroEvents.withMetadata(AvroEvents.fromJson(
                "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":960,\"bookName\":\"Once\",\"bookAuthor\":\"Id\"}}"), metadata);

        kafkaTemplate.executeInTransaction(t -> t.sendDefault(event));
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(event));   // same eventId, new offset

        // both deliveries reach the service; the second is skipped — no book_id violation, no DLT
        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(libraryEventsServiceSpy, times(2)).processLibraryEvent(argThat(r -> event.equals(r.value()))));
        assertThat(libraryEventsRepository.findAll()).filteredOn(e -> e.getBook().getBookId() == 960).hasSize(1);
        assertThat(jdbcTemplate.queryForObject(
                "select count(*) from processed_event where event_id = ?", Integer.class, metadata.eventId()))
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Idempotent consumer: the same record delivered twice is applied once")
    void redeliveredRecord_isAppliedOnce() {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":789,\"bookName\":\"Exactly Once\",\"bookAuthor\":\"Kafka\"}}";
        // Same topic/partition/offset = a redelivery after the DB committed but the offset commit failed
        var record = new ConsumerRecord<Integer, LibraryEventAvro>("library-events", 0, 999_999L, null, AvroEvents.fromJson(json));

        libraryEventsServiceSpy.processLibraryEvent(record);
        libraryEventsServiceSpy.processLibraryEvent(record);   // would hit book_id PK without the dedupe

        assertThat(libraryEventsRepository.findAll())
                .extracting(e -> e.getBook().getBookId())
                .containsExactly(789);
    }

    @Test
    @DisplayName("A CoffeeOrder published as Avro (String key) reaches the coffee-orders listener as a typed record")
    void coffeeOrder_isConsumed() {
        var order = AvroEvents.coffeeOrder("Ada");
        sendCoffee(order);

        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(coffeeOrdersConsumerSpy).onMessage(argThat(r ->
                        r.key().equals(order.getId().toString()) && order.equals(r.value()))));
    }

    @Test
    @DisplayName("Retry topics: an order that fails on the main topic is retried on coffee-orders-retry-0 and succeeds")
    void flakyCoffeeOrder_succeedsOnRetryTopic() {
        var order = AvroEvents.coffeeOrder("flaky Linus");
        sendCoffee(order);

        Awaitility.await().atMost(20, TimeUnit.SECONDS).untilAsserted(() ->
                verify(coffeeOrdersConsumerSpy).onMessage(argThat(r ->
                        "coffee-orders-retry-0".equals(r.topic()) && order.equals(r.value()))));
    }

    @Test
    @DisplayName("Retry topics: an order that always fails goes through every retry topic and lands on coffee-orders-dlt")
    void failingCoffeeOrder_endsOnDlt() {
        var order = AvroEvents.coffeeOrder("fail Ken");
        sendCoffee(order);

        // 1 s + 2 s + 4 s of back-off across coffee-orders-retry-0/1/2
        ConsumerRecord<byte[], byte[]> dead = awaitRecord("coffee-orders-dlt", Duration.ofSeconds(40),
                bytes -> order.equals(AvroEvents.decode("coffee-orders-dlt", bytes)));
        assertThat(dead).isNotNull();
        verify(coffeeOrdersConsumerSpy, times(4)).onMessage(argThat(r -> order.equals(r.value())));
    }

    @Test
    @DisplayName("Compacted topic: a processed event publishes its latest state, keyed by the database id")
    void processedEvent_publishesLatestState() {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":901,\"bookName\":\"State\",\"bookAuthor\":\"Compact\"}}";
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(AvroEvents.fromJson(json)));

        ConsumerRecord<byte[], byte[]> state = awaitRecord(LibraryEventsService.STATE_TOPIC, Duration.ofSeconds(15),
                bytes -> AvroEvents.decode(LibraryEventsService.STATE_TOPIC, bytes) instanceof LibraryEventAvro e
                        && e.getBook().getBookId() == 901);
        var saved = libraryEventsRepository.findAll().stream()
                .filter(e -> e.getBook().getBookId() == 901).findFirst().orElseThrow();
        try (var keys = new IntegerDeserializer()) {
            assertThat(keys.deserialize(LibraryEventsService.STATE_TOPIC, state.key())).isEqualTo(saved.getLibraryEventId());
        }
    }

    @Test
    @DisplayName("Share group: a barista RELEASEs a 'busy' order, it is redelivered (delivery count 2) and accepted")
    void shareGroup_releasedOrderIsRedelivered() {
        var order = AvroEvents.coffeeOrder("busy Barbara");
        sendCoffee(order);

        Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(baristaSpy).onMessage(argThat(r -> order.equals(r.value()) && r.deliveryCount().orElse((short) 0) == 1), any());
            verify(baristaSpy).onMessage(argThat(r -> order.equals(r.value()) && r.deliveryCount().orElse((short) 0) == 2), any());
        });
    }

    @Test
    @DisplayName("Replay: seeking a partition back redelivers its records, and the idempotent consumer skips them")
    void replay_redeliversAndIsSkipped() {
        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":902,\"bookName\":\"Replay\",\"bookAuthor\":\"Seek\"}}";
        var event = AvroEvents.fromJson(json);
        kafkaTemplate.executeInTransaction(t -> t.sendDefault(event));
        var captor = ArgumentCaptor.forClass(ConsumerRecord.class);
        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(libraryEventsServiceSpy).processLibraryEvent(argThat(r -> event.equals(r.value()))));
        verify(libraryEventsServiceSpy, atLeastOnce()).processLibraryEvent(captor.capture());
        ConsumerRecord<?, ?> original = captor.getAllValues().stream()
                .filter(r -> event.equals(r.value())).findFirst().orElseThrow();

        assertThat(libraryEventsConsumerSpy.replay(original.partition(), original.offset())).isTrue();

        Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(libraryEventsServiceSpy, times(2)).processLibraryEvent(argThat(r -> event.equals(r.value()))));
        assertThat(libraryEventsRepository.findAll()).filteredOn(e -> e.getBook().getBookId() == 902).hasSize(1);
    }

    /** Sends a CoffeeOrder with its UUID as a String key. */
    private void sendCoffee(CoffeeOrder order) {
        // A String-keyed copy of the app's (transactional) producer factory. It needs its OWN
        // transaction-id prefix — reusing the app's would fence the app's producer
        // (same transactional.id, newer epoch → InvalidProducerEpochException on its next send).
        var stringKeyedFactory = (DefaultKafkaProducerFactory<Object, Object>) producerFactory.copyWithConfigurationOverride(
                Map.of(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
        stringKeyedFactory.setTransactionIdPrefix("coffee-test-tx-");
        var stringKeyed = new KafkaTemplate<>(stringKeyedFactory);
        try {
            stringKeyed.executeInTransaction(t -> t.send(CoffeeOrdersConsumer.TOPIC, order.getId().toString(), order));
        } finally {
            stringKeyedFactory.destroy();
        }
    }

    /** Reads a topic (raw byte keys and values) from the beginning until a value matches. */
    private ConsumerRecord<byte[], byte[]> awaitRecord(String topic, Duration timeout, Predicate<byte[]> matches) {
        var props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "reader-" + System.nanoTime(), false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<byte[], byte[]> reader = new DefaultKafkaConsumerFactory<>(
                props, new ByteArrayDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            reader.subscribe(List.of(topic));
            var found = new AtomicReference<ConsumerRecord<byte[], byte[]>>();
            Awaitility.await().atMost(timeout).until(() -> {
                for (ConsumerRecord<byte[], byte[]> r : reader.poll(Duration.ofMillis(500))) {
                    if (matchesSafely(matches, r.value())) {
                        found.set(r);
                    }
                }
                return found.get() != null;
            });
            return found.get();
        }
    }

    /** Reads library-events.DLT (as raw bytes) from the beginning until a value matches. */
    private ConsumerRecord<Integer, byte[]> awaitDeadLetter(Predicate<byte[]> matches) {
        var props = KafkaTestUtils.consumerProps(embeddedKafkaBroker, "dlt-reader-" + System.nanoTime(), false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (Consumer<Integer, byte[]> dltConsumer = new DefaultKafkaConsumerFactory<>(
                props, new IntegerDeserializer(), new ByteArrayDeserializer()).createConsumer()) {
            dltConsumer.subscribe(List.of(DLT));
            var found = new AtomicReference<ConsumerRecord<Integer, byte[]>>();
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() -> {
                for (ConsumerRecord<Integer, byte[]> r : dltConsumer.poll(Duration.ofMillis(500))) {
                    if (matchesSafely(matches, r.value())) {
                        found.set(r);
                    }
                }
                return found.get() != null;
            });
            return found.get();
        }
    }

    /** The DLT mixes Avro and raw bytes; a decode failure just means "not this record". */
    private static boolean matchesSafely(Predicate<byte[]> matches, byte[] value) {
        try {
            return matches.test(value);
        } catch (RuntimeException notAvro) {
            return false;
        }
    }
}
