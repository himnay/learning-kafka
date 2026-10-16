package com.learnkafka.coffee;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CoffeeTopicCheck.run() against a real broker whose coffee-orders topic is too small. */
@EmbeddedKafka(topics = CoffeeOrderService.TOPIC, partitions = 2)
class CoffeeTopicCheckEmbeddedKafkaTest {

    @Test
    @DisplayName("coffee-orders has fewer partitions than app.coffee.partitions: the app refuses to start")
    void topicWithFewerPartitions_refusesToStart(EmbeddedKafkaBroker broker) {
        var admin = new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString()));

        assertThatThrownBy(() -> new CoffeeTopicCheck(admin, 3).run(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has 2 partitions but app.coffee.partitions is 3");
    }
}
