package com.learnkafka.consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.learnkafka.AvroEvents;
import com.learnkafka.TestcontainersConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** The "manual-offset" profile swaps in the manual-ack listener, and its acknowledge() commits the offset. */
@SpringBootTest
@ActiveProfiles("manual-offset")
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(topics = {"library-events", "coffee-orders"}, partitions = 3, brokerProperties = {
        // single embedded broker: the transaction state log must fit on one node
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1"
})
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=" + AvroEvents.REGISTRY,
        "app.coffee.share-group.enabled=false"   // share groups are covered in LibraryEventsConsumerIntegrationTest
})
class LibraryEventsConsumerManualOffsetTest {

    @Autowired
    ApplicationContext context;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, Object> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Test
    @DisplayName("only the manual-offset listener runs, and acknowledging commits the record's offset")
    void manualAcknowledge_commitsOffset() throws Exception {
        assertThat(context.getBeansOfType(LibraryEventsConsumer.class)).isEmpty();
        assertThat(context.getBeansOfType(LibraryEventsConsumerManualOffset.class)).hasSize(1);
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            // share-group containers have no partition assignment to wait for
            if (container instanceof ConcurrentMessageListenerContainer<?, ?>) {
                ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
            }
        }

        kafkaTemplate.executeInTransaction(t -> t.sendDefault(1, AvroEvents.fromJson("{\"libraryEventId\":1,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":1,\"bookName\":\"Manual\",\"bookAuthor\":\"Ack\"}}"))).get();

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString()))) {
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
                    admin.listConsumerGroupOffsets("library-events-listener-group")
                            .partitionsToOffsetAndMetadata().get().values().stream()
                            .mapToLong(OffsetAndMetadata::offset).sum() == 1);
        }
    }
}
