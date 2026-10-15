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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * POST /v1/key-routing/partitions/{partition} against a real 3-partition topic. Its own broker:
 * KeyRoutingIntegrationTest counts every record on the topic, so it can't share one.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(topics = KeyRoutingProducer.TOPIC, partitions = 3)
@DirtiesContext
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=mock://explicit-partition-test",
        "app.key-routing.substring-partitions=VIP=0,UK,US"
})
class ExplicitPartitionIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    @DisplayName("A VIP key sent to partition 2 lands on 2: an explicit partition bypasses the partitioner (VIP=0)")
    void explicitPartitionBeatsThePartitioner() {
        var response = restClient().post().uri("/v1/key-routing/partitions/2")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new KeyRoutingRequest("order-VIP-9", "pinned by the caller"))
                .retrieve()
                .body(KeyRoutingResponse.class);

        assertThat(response.partition()).isEqualTo(2);
        assertThat(response.matchedSubstring()).isNull();

        var consumerProps = new HashMap<String, Object>(KafkaTestUtils.consumerProps(embeddedKafkaBroker, "explicit-partition-test", true));
        try (var consumer = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer())
                .createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, KeyRoutingProducer.TOPIC);
            var record = KafkaTestUtils.getSingleRecord(consumer, KeyRoutingProducer.TOPIC, Duration.ofSeconds(10));
            assertThat(record.partition()).isEqualTo(2);
            assertThat(record.offset()).isEqualTo(response.offset());
            assertThat(record.key()).isEqualTo("order-VIP-9");
        }
    }

    @Test
    @DisplayName("A partition the topic doesn't have is rejected at once with 400, instead of send() blocking for 60 s")
    void missingPartitionFailsFast() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
                assertThatThrownBy(() -> restClient().post().uri("/v1/key-routing/partitions/3")
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(new KeyRoutingRequest("order-1", "nowhere to go"))
                        .retrieve()
                        .toBodilessEntity())
                        .isInstanceOf(HttpClientErrorException.BadRequest.class));
    }

    private RestClient restClient() {
        return RestClient.create("http://localhost:" + port);
    }
}
