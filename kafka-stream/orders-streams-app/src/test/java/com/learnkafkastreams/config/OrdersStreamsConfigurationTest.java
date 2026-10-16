package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsSerializationExceptionHandler;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.SerializationExceptionOrigin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** The streams config bean replaces Boot's, so every exception handler must be registered in it. */
class OrdersStreamsConfigurationTest {

    private final Properties props = new OrdersStreamsConfiguration()
            .kStreamConfig("localhost:9092", "orders-test", "", new KafkaProperties())
            .asProperties();

    @Test
    @DisplayName("deserialization and production exception handlers are registered with Kafka Streams")
    void registersExceptionHandlers() {
        StreamsConfig config = new StreamsConfig(props);

        assertThat(config.getConfiguredInstance(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                DeserializationExceptionHandler.class))
                .isInstanceOf(RecoveringDeserializationExceptionHandler.class);
        assertThat(config.getConfiguredInstance(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                ProductionExceptionHandler.class))
                .isInstanceOf(StreamsSerializationExceptionHandler.class);
    }

    @Test
    @DisplayName("SASL/SSL settings from spring.kafka.* reach Kafka Streams (this bean replaces Boot's, so it copies them)")
    void securitySettingsReachKafkaStreams() {
        var jaas = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"orders-streams\" password=\"x\";";
        var kafka = new KafkaProperties();
        kafka.getSecurity().setProtocol("SASL_SSL");
        kafka.getProperties().put("sasl.mechanism", "SCRAM-SHA-512");
        kafka.getProperties().put("sasl.jaas.config", jaas);
        kafka.getSsl().setTrustStoreLocation(new FileSystemResource("/tmp/kafka.truststore.p12"));
        kafka.getSsl().setTrustStoreType("PKCS12");

        var secured = new OrdersStreamsConfiguration().kStreamConfig("localhost:9092", "orders-test", "", kafka).asProperties();

        assertThat(secured)
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsEntry("sasl.jaas.config", jaas)
                .containsEntry("ssl.truststore.type", "PKCS12");
        assertThat(secured.getProperty("ssl.truststore.location")).endsWith("kafka.truststore.p12");
    }

    @Test
    @DisplayName("without security settings Kafka Streams connects in plaintext")
    void plaintextByDefault() {
        assertThat(props).doesNotContainKey("sasl.mechanism").doesNotContainKey("security.protocol");
    }

    @Test
    @DisplayName("the Streams producer zstd-compresses what it writes")
    void producerCompressesBatches() {
        var producerConfigs = new StreamsConfig(props).getProducerConfigs("orders-test-producer");

        assertThat(producerConfigs).containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    }

    @Test
    @DisplayName("a record that fails to serialize is skipped, not fatal")
    void serializationFailure_isSkipped() {
        var response = new StreamsSerializationExceptionHandler().handleSerializationError(
                mock(ErrorHandlerContext.class), new ProducerRecord<>("orders-count", "key", "value"),
                new IllegalStateException("boom"), SerializationExceptionOrigin.VALUE);

        assertThat(response.result()).isEqualTo(ProductionExceptionHandler.Result.RESUME);
    }
}
