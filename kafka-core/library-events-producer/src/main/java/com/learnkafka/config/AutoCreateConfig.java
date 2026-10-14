package com.learnkafka.config;

import com.learnkafka.producer.LibraryEventProducer;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares library-events so the app also works against a bare broker (e.g. embedded Kafka in tests).
 * With the kafka-core docker compose, kafka-init has already created it with RF 3, so this is a no-op.
 */
@Configuration
public class AutoCreateConfig {

    /** Defines the library events bean. */
    @Bean
    public NewTopic libraryEvents() {
        // No .replicas(): the broker's default.replication.factor applies (3 on the compose
        // cluster, 1 on a single-node test broker) instead of a hard-coded, unsafe RF 1.
        return TopicBuilder.name(LibraryEventProducer.TOPIC)
                .partitions(3)
                .build();
    }
}
