package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.HashMap;

/**
 * Kafka Streams configuration.
 *
 * GoF patterns applied:
 *   - Strategy      : pluggable ConsumerRecordRecoverer (log-and-skip vs DLQ)
 *   - Factory Method : @Bean methods produce configured objects consumed by the framework
 *   - Decorator     : StreamsBuilderFactoryBeanConfigurer wraps the factory bean
 */
@Slf4j
@Configuration
public class OrdersStreamsConfiguration {

    /** Log-and-skip: records that fail inline processing are logged and discarded. */
    private final ConsumerRecordRecoverer logAndSkipRecoverer = (record, ex) ->
            log.error("Skipping failed record: record={} ex={}", record, ex.getMessage(), ex);

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig(
            @Value("${spring.kafka.streams.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String applicationId) {
        var props = new HashMap<String, Object>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(OrdersTopology.ORDERS).partitions(2).replicas(1).build();
    }

    @Bean
    public NewTopic storesTopic() {
        return TopicBuilder.name(OrdersTopology.STORES).partitions(2).replicas(1).build();
    }
}
