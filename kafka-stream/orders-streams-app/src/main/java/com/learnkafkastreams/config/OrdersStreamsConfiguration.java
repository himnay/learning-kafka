package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.exceptionhandler.StreamsSerializationExceptionHandler;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
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

    /** Defines the k stream config bean. */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig(
            @Value("${spring.kafka.streams.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.streams.application-id}") String applicationId,
            @Value("${spring.kafka.streams.state-dir:}") String stateDir,
            KafkaProperties kafkaProperties) {
        var props = new HashMap<String, Object>();
        // This bean replaces Boot's streams configuration, so the shared client settings have to be copied
        // in: spring.kafka.properties (sasl.mechanism, sasl.jaas.config), security.protocol and the
        // truststore — SASL_SSL to the kafka-core cluster's host listener.
        props.putAll(kafkaProperties.getProperties());
        props.putAll(kafkaProperties.getSecurity().buildProperties());
        props.putAll(kafkaProperties.getSsl().buildProperties());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        // KTables are cached and only flushed DOWNSTREAM on commit (default every 30 s with at-least-once).
        // The revenue ⋈ stores join sits downstream of the cached revenue table, so commit every second
        // to make joined results visible within ~1 s. Interactive queries on the aggregate stores themselves
        // read through the cache and are always current.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1_000);
        if (!stateDir.isBlank()) {
            // where RocksDB keeps the state stores (default: <java.io.tmpdir>/kafka-streams)
            props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        }
        // Compress everything Streams writes (output, changelog and repartition topics) in its producer,
        // before sending, like the kafka-core apps. Streams already lingers 100 ms, so batches are big.
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.RECOVERER,
                logAndSkipRecoverer);
        // This bean replaces Boot's streams configuration, so spring.kafka.* properties do not
        // reach Kafka Streams: every handler has to be registered here.
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamsSerializationExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

    /** Defines the streams builder factory bean configurer bean. */
    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer() {
        return factoryBean -> {
            log.info("Registering custom uncaught exception handler on StreamsBuilderFactoryBean");
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
        };
    }

    /** Defines the orders topic bean. */
    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(OrdersTopology.ORDERS).partitions(2).build();   // RF: broker default
    }

    /** Defines the stores topic bean. */
    @Bean
    public NewTopic storesTopic() {
        return TopicBuilder.name(OrdersTopology.STORES).partitions(2).build();   // RF: broker default
    }
}
