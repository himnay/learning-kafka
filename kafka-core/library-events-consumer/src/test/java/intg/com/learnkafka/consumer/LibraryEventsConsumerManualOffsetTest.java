package com.learnkafka.consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
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
@ActiveProfiles({"local", "manual-offset"})
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false"
})
class LibraryEventsConsumerManualOffsetTest {

    @Autowired
    ApplicationContext context;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @Test
    @DisplayName("only the manual-offset listener runs, and acknowledging commits the record's offset")
    void manualAcknowledge_commitsOffset() throws Exception {
        assertThat(context.getBeansOfType(LibraryEventsConsumer.class)).isEmpty();
        assertThat(context.getBeansOfType(LibraryEventsConsumerManualOffset.class)).hasSize(1);
        for (MessageListenerContainer container : endpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }

        kafkaTemplate.sendDefault(1, "{\"libraryEventId\":1}").get();

        try (Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString()))) {
            Awaitility.await().atMost(15, TimeUnit.SECONDS).until(() ->
                    admin.listConsumerGroupOffsets("library-events-listener-group")
                            .partitionsToOffsetAndMetadata().get().values().stream()
                            .mapToLong(OffsetAndMetadata::offset).sum() == 1);
        }
    }
}
