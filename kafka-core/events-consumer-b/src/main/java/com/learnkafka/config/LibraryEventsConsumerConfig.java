package com.learnkafka.config;

import com.learnkafka.event.EventMetadataMdcInterceptor;
import com.learnkafka.retry.RetryProperties;
import com.learnkafka.retry.RetryRouter;
import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import jakarta.persistence.EntityManagerFactory;

import java.util.Map;

/**
 * Kafka consumer container configuration.
 *
 * Transactions (exactly-once from Kafka's side, idempotent on the database side):
 *   - The container starts a KAFKA transaction per record (KafkaTransactionManager);
 *     the consumed offset is committed inside it (sendOffsetsToTransaction), together with
 *     anything published to Kafka while processing (state topic, retry topic, DLT).
 *   - The service runs its own DATABASE transaction (JpaTransactionManager, @Primary).
 *     It commits first; the Kafka transaction commits after the listener returns.
 *     If the Kafka commit then fails, the record is redelivered and the
 *     processed_event table (idempotent consumer) skips it.
 *   - Failures roll the Kafka transaction back; the AfterRollbackProcessor retries the
 *     record, then recovers it (retry topic / DLT, see RetryRouter) and commits its offset in a
 *     new transaction. RetryTopicDrainer retries what lands on the retry topic.
 *
 * GoF patterns applied:
 *   - Strategy      : pluggable recoverer (RetryRouter) injected into the AfterRollbackProcessor
 *   - Template Method : RetryRouter branches on exception type and attempt count
 *   - Factory Method  : @Bean creates the ConcurrentKafkaListenerContainerFactory
 */
@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

    static final String DLT_TOPIC = "library-events.DLT";

    /** Container factory for listeners that commit offsets themselves. */
    public static final String MANUAL_ACK_CONTAINER_FACTORY = "manualAckContainerFactory";

    private final RetryProperties retryProperties;
    private final KafkaTemplate<Integer, Object> kafkaTemplate;
    private final KafkaTransactionManager<?, ?> kafkaTransactionManager;

    /** Listener threads in THIS app; the partitions are shared with the other consumer app. */
    @Value("${spring.kafka.listener.concurrency:1}")
    private int concurrency;

    /**
     * Database transaction manager. Declared explicitly and @Primary because a
     * KafkaTransactionManager bean also exists (spring.kafka.producer.transaction-id-prefix);
     * without this, Boot's JPA transaction manager could back off and @Transactional
     * would silently run against Kafka instead of the database.
     */
    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    /**
     * Value serializer for everything this app publishes (state topic, retry topic, DLT), chosen by type:
     *  - Avro records (SpecificRecord) → KafkaAvroSerializer, schema registered in Schema Registry
     *  - byte[]                       → ByteArraySerializer: a record that failed Avro
     *                                   deserialization is dead-lettered with its original bytes
     * Both delegates are configured from the producer properties (incl. schema.registry.url).
     * Static: the producer factory needs it before this config class (which needs the
     * KafkaTemplate) can be created — an instance method would be a circular reference.
     */
    @Bean
    @SuppressWarnings("unchecked")
    public static DefaultKafkaProducerFactoryCustomizer avroOrBytesValueSerializer() {
        return producerFactory -> ((DefaultKafkaProducerFactory<Object, Object>) producerFactory)
                .setValueSerializerSupplier(() -> new DelegatingByTypeSerializer(Map.of(
                        byte[].class, new ByteArraySerializer(),
                        SpecificRecord.class, new KafkaAvroSerializer()), true));
    }

    /** Default container factory; concurrency from spring.kafka.listener.concurrency. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        return newContainerFactory(kafkaConsumerFactory);
    }

    /**
     * Same container setup, but the listener commits each offset itself via
     * {@code Acknowledgment.acknowledge()}, synchronously (MANUAL_IMMEDIATE).
     */
    @Bean(MANUAL_ACK_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> manualAckContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var factory = newContainerFactory(kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        return factory;
    }

    private ConcurrentKafkaListenerContainerFactory<Object, Object> newContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setConcurrency(concurrency);
        factory.setRecordInterceptor(new EventMetadataMdcInterceptor());   // base-event metadata → MDC
        // Kafka transaction per record: offsets + any sends commit atomically
        factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
        factory.setAfterRollbackProcessor(afterRollbackProcessor());
        return factory;
    }

    /**
     * With a transactional container the AfterRollbackProcessor (not the CommonErrorHandler)
     * decides what happens after a failed record rolls the transaction back.
     * - FixedBackOff: 1 s delay, 2 retries (3 total attempts)
     * - IllegalArgumentException and DataIntegrityViolationException are not retried — straight to the recoverer
     * - commitRecovered=true: the recoverer's send (retry topic / DLT) and the recovered
     *   record's offset commit in ONE new Kafka transaction, so it is never reprocessed
     */
    private DefaultAfterRollbackProcessor<Object, Object> afterRollbackProcessor() {
        var backOff = new FixedBackOff(1_000L, 2);
        var processor = new DefaultAfterRollbackProcessor<>(retryRouter(), backOff, kafkaTemplate, true);
        // Deterministic failures never succeed on a retry: invalid events, and constraint violations
        // such as a NEW event for a book_id that already exists
        processor.addNotRetryableExceptions(IllegalArgumentException.class, DataIntegrityViolationException.class);
        return processor;
    }

    /**
     * The retry rule (shared with RetryTopicDrainer): RecoverableDataAccessException → library-events.retry
     * with retry-attempt 1; anything else → DLT. A record keeps its partition number either way.
     */
    @Bean
    public RetryRouter retryRouter() {
        return new RetryRouter(kafkaTemplate, retryProperties, DLT_TOPIC);
    }

    /** Retry topic: same partition count as library-events, so a forwarded record keeps its partition. */
    @Bean
    public NewTopic libraryEventsRetry() {
        return TopicBuilder.name(retryProperties.topic()).partitions(3).build();   // RF: broker default
    }

    /** Defines the library events dlt bean. */
    @Bean
    public NewTopic libraryEventsDlt() {
        return TopicBuilder.name(DLT_TOPIC).partitions(3).build();   // RF: broker default
    }

    /**
     * COMPACTED topic holding the latest state of every library event, keyed by libraryEventId.
     * Compaction keeps only the newest record per key, so it reads like a table: "current book
     * for each id". Written by LibraryEventsService inside the listener's Kafka transaction.
     * With docker compose, kafka-init has already created it (RF 3), so this is a no-op there.
     */
    @Bean
    public NewTopic libraryBookState() {
        return TopicBuilder.name(LibraryEventsService.STATE_TOPIC).partitions(3).compact().build();   // RF: broker default
    }
}
