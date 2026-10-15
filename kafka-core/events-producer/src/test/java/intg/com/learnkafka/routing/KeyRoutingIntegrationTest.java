package com.learnkafka.routing;

import com.learnkafka.TestcontainersConfiguration;
import org.springframework.context.annotation.Import;
import com.learnkafka.domain.KeyRoutingRequest;
import com.learnkafka.domain.KeyRoutingResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** The real partitioner inside a real producer: where do records land on a 3-partition topic? */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(topics = KeyRoutingProducer.TOPIC, partitions = 3)
@DirtiesContext
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=mock://key-routing-test",
        "app.key-routing.substring-partitions=VIP=0,UK,US"
})
class KeyRoutingIntegrationTest {

    private static final List<String> VIP_KEYS = List.of("VIP-1", "order-VIP-2", "gold-VIP", "VIP-US-3");
    private static final List<String> UK_KEYS = List.of("UK-1", "branch-UK-2", "order-UK-3");
    private static final List<String> US_KEYS = List.of("US-1", "order-US-2", "branch-US-3");

    @LocalServerPort
    int port;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    @DisplayName("Keys that share a substring land on one partition; VIP is pinned to 0, UK and US hash to 1 and 2")
    void keysSharingASubstringShareAPartition() {
        var restClient = RestClient.create("http://localhost:" + port);
        var responses = new HashMap<String, KeyRoutingResponse>();
        for (var key : concat(VIP_KEYS, UK_KEYS, US_KEYS, List.of("order-12345"))) {
            responses.put(key, restClient.post().uri("/v1/key-routing")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new KeyRoutingRequest(key, "message for " + key))
                    .retrieve()
                    .body(KeyRoutingResponse.class));
        }

        assertThat(VIP_KEYS).allSatisfy(key -> assertThat(responses.get(key).partition()).isZero());
        assertThat(UK_KEYS).allSatisfy(key -> assertThat(responses.get(key).partition()).isEqualTo(1));
        assertThat(US_KEYS).allSatisfy(key -> assertThat(responses.get(key).partition()).isEqualTo(2));
        assertThat(responses.get("VIP-US-3").matchedSubstring()).isEqualTo("VIP");   // first rule wins
        assertThat(responses.get("order-12345").matchedSubstring()).isNull();

        // The topic agrees with the responses: every record sits on the partition its response reported
        var consumerProps = new HashMap<String, Object>(KafkaTestUtils.consumerProps(embeddedKafkaBroker, "key-routing-test", true));
        try (var consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer())
                .createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, KeyRoutingProducer.TOPIC);
            var records = KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10), responses.size());
            assertThat(records.count()).isEqualTo(responses.size());
            records.forEach(record -> {
                assertThat(record.partition()).isEqualTo(responses.get(record.key()).partition());
                assertThat(record.offset()).isEqualTo(responses.get(record.key()).offset());
                assertThat(record.value()).isEqualTo("message for " + record.key());
            });
        }
    }

    @SafeVarargs
    private static List<String> concat(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toList();
    }
}
