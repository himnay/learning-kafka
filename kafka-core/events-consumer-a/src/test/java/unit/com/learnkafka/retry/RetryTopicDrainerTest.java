package com.learnkafka.retry;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.DeserializationException;

import java.net.ConnectException;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RetryTopicDrainer: its own consumer settings, and Schema Registry outages vs bad records")
class RetryTopicDrainerTest {

    @Test
    @DisplayName("the drain consumer never auto-commits and reads committed records only, whatever the shared config says")
    void drainConsumer_pinsCommitAndIsolationSettings() {
        assertThat(RetryTopicDrainer.readerProperties())
                .containsEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .containsEntry(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");
    }

    @Test
    @DisplayName("Schema Registry unreachable (connection refused) is an outage: leave the record for the next run")
    void registryConnectionRefused_isAnOutage() {
        assertThat(RetryTopicDrainer.isRegistryUnreachable(notAvro(
                new SerializationException("Error retrieving Avro schema for id 1", new ConnectException("Connection refused")))))
                .isTrue();
    }

    @Test
    @DisplayName("Schema Registry answering 5xx is an outage too")
    void registryServerError_isAnOutage() {
        assertThat(RetryTopicDrainer.isRegistryUnreachable(notAvro(
                new SerializationException("Error retrieving Avro schema", new RestClientException("Internal error", 500, 50001)))))
                .isTrue();
    }

    @Test
    @DisplayName("an unknown schema id or bytes that are not Avro are bad records: they go to the DLT")
    void badRecord_isNotAnOutage() {
        assertThat(RetryTopicDrainer.isRegistryUnreachable(notAvro(
                new SerializationException("Error retrieving Avro schema", new RestClientException("Schema not found", 404, 40403)))))
                .isFalse();
        assertThat(RetryTopicDrainer.isRegistryUnreachable(notAvro(new SerializationException("Unknown magic byte!"))))
                .isFalse();
    }

    private static DeserializationException notAvro(Throwable cause) {
        return new DeserializationException("failed to deserialize", new byte[]{1, 2, 3}, false, cause);
    }
}
