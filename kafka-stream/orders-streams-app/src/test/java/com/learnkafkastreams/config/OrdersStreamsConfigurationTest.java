package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsSerializationExceptionHandler;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler.SerializationExceptionOrigin;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** The streams config bean replaces Boot's, so every exception handler must be registered in it. */
class OrdersStreamsConfigurationTest {

    private final Properties props = new OrdersStreamsConfiguration()
            .kStreamConfig("localhost:9092", "orders-test")
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
    @DisplayName("a record that fails to serialize is skipped, not fatal")
    void serializationFailure_isSkipped() {
        var response = new StreamsSerializationExceptionHandler().handleSerializationError(
                mock(ErrorHandlerContext.class), new ProducerRecord<>("orders-count", "key", "value"),
                new IllegalStateException("boom"), SerializationExceptionOrigin.VALUE);

        assertThat(response.result()).isEqualTo(ProductionExceptionHandler.Result.RESUME);
    }
}
