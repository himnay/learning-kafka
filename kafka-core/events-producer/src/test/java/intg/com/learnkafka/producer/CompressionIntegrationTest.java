package com.learnkafka.producer;

import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.event.EventMetadataFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.util.HashMap;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compression happens in the producer, before the send: each batch is zstd-compressed as one block.
 * The producer metric compression-rate-avg (compressed batch size / raw size) shows the effect.
 */
@SpringBootTest
@EmbeddedKafka(topics = LibraryEventProducer.TOPIC, partitions = 3)
@DirtiesContext
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=" + CompressionIntegrationTest.REGISTRY,
        // Let the whole burst fill one batch, so the ratio doesn't depend on how fast the loop runs
        "spring.kafka.producer.properties.linger.ms=1000",
        "spring.kafka.producer.properties.batch.size=1048576"
})
class CompressionIntegrationTest {

    static final String REGISTRY = "mock://compression-test";

    private static final int RECORDS = 500;

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @Autowired
    KafkaTemplate<Integer, LibraryEventAvro> kafkaTemplate;

    @Autowired
    EventMetadataFactory eventMetadataFactory;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    @DisplayName("Batches leave the producer zstd-compressed to well under half their size, and consumers read them back unchanged")
    void batchesAreCompressedBeforeSending() {
        assertThat(kafkaTemplate.getProducerFactory().getConfigurationProperties())
                .containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        for (int i = 0; i < RECORDS; i++) {
            libraryEventProducer.sendLibraryEvent(newEvent(i));
        }
        kafkaTemplate.flush();

        double ratio = (double) kafkaTemplate.metrics().entrySet().stream()
                .filter(e -> e.getKey().group().equals("producer-metrics")
                        && e.getKey().name().equals("compression-rate-avg"))
                .findFirst().orElseThrow()
                .getValue().metricValue();
        assertThat(ratio).isBetween(0.0, 0.5);

        var props = new HashMap<>(KafkaTestUtils.consumerProps(embeddedKafkaBroker, "compression-test", true));
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        @SuppressWarnings("unchecked")
        var avroDeserializer = (Deserializer<LibraryEventAvro>) (Deserializer<?>) new KafkaAvroDeserializer();
        try (var consumer = new DefaultKafkaConsumerFactory<>(props, new IntegerDeserializer(), avroDeserializer)
                .createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, LibraryEventProducer.TOPIC);
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), RECORDS);
            assertThat(StreamSupport.stream(records.spliterator(), false)
                    .map(record -> record.value().getBook().getBookId()))
                    .hasSize(RECORDS)
                    .containsExactlyInAnyOrderElementsOf(IntStream.range(0, RECORDS).boxed().toList());
        }
    }

    private LibraryEvent newEvent(int bookId) {
        return LibraryEvent.builder()
                .metadata(eventMetadataFactory.newEvent(LibraryEvent.TYPE_PREFIX + LibraryEventType.NEW, null))
                .libraryEventType(LibraryEventType.NEW)
                .book(Book.builder().bookId(bookId).bookName("Kafka using Spring Boot").bookAuthor("Dilip").build())
                .build();
    }
}
