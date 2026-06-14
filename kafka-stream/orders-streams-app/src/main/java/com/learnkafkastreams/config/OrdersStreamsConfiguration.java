package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

/**
 * Kafka Streams configuration.
 *
 * GoF patterns applied:
 *   - Strategy      : pluggable ConsumerRecordRecoverer (DLQ vs log-and-skip)
 *   - Factory Method : @Bean methods produce configured objects consumed by the framework
 *   - Decorator     : StreamsBuilderFactoryBeanConfigurer wraps the factory bean
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class OrdersStreamsConfiguration {

    private final KafkaProperties kafkaProperties;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig() {
        var props = kafkaProperties.buildStreamsProperties(null);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER,
                logAndSkipRecoverer);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
        return factoryBean -> {
            log.info("Registering custom uncaught exception handler on StreamsBuilderFactoryBean");
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
        };
    }

    /** Routes unprocessable records to a DLQ topic instead of crashing the stream. */
    private DeadLetterPublishingRecoverer dlqRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Deserialization error, routing to DLQ: record={} ex={}", record, ex.getMessage(), ex);
                    return new TopicPartition("orders-DLQ", record.partition());
                });
    }

    /** Log-and-skip: records that fail inline processing are logged and discarded. */
    private final ConsumerRecordRecoverer logAndSkipRecoverer = (record, ex) ->
            log.error("Skipping failed record: record={} ex={}", record, ex.getMessage(), ex);

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(OrdersTopology.ORDERS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic storesTopic() {
        return TopicBuilder.name(OrdersTopology.STORES).partitions(2).replicas(1).build();
    }
}
