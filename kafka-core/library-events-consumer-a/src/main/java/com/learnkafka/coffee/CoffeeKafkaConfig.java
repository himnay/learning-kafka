package com.learnkafka.coffee;

import com.learnkafka.event.EventMetadataMdcInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.ShareAckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.List;

/**
 * Kafka wiring for the coffee-orders topic: a plain consumer-group container (with NON-BLOCKING
 * retry topics, see CoffeeOrdersConsumer), and a SHARE-group container for the barista work queue.
 */
@Configuration
public class CoffeeKafkaConfig {

    /** Plain (non-transactional) container factory for the coffee-orders listener. */
    public static final String COFFEE_ORDERS_CONTAINER_FACTORY = "coffeeOrdersContainerFactory";

    /** Non-transactional, String-keyed template for coffee retry topics and the coffee DLT. */
    public static final String COFFEE_RETRY_TEMPLATE = "coffeeRetryKafkaTemplate";

    /** Share-group container factory for the barista listener. */
    public static final String BARISTA_SHARE_CONTAINER_FACTORY = "baristaShareContainerFactory";

    /** Configs a share consumer refuses ("... cannot be set when using a share group"). */
    private static final List<String> SHARE_GROUP_UNSUPPORTED = List.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG);

    @Value("${spring.kafka.listener.concurrency:1}")
    private int concurrency;

    /**
     * Coffee orders only log, so they get a simple container: no Kafka transaction.
     * @RetryableTopic on CoffeeOrdersConsumer replaces its error handling with retry topics.
     */
    @Bean(COFFEE_ORDERS_CONTAINER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> coffeeOrdersContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setConcurrency(concurrency);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1_000L, 2)));
        factory.setRecordInterceptor(new EventMetadataMdcInterceptor());   // base-event metadata → MDC (retry topics too)
        return factory;
    }

    /**
     * Template for the NON-BLOCKING retry/DLT records of coffee-orders (see CoffeeOrdersConsumer's
     * @RetryableTopic): String keys (order UUIDs) and NO transactions — the coffee container isn't
     * transactional. Built from Boot's producer settings and the Avro/bytes value serializer.
     *
     * defaultCandidate = false: only injected/looked up BY NAME. Boot's @ConditionalOnMissingBean
     * ignores it, so Boot's auto-configured KafkaTemplate (used for library events) stays in place.
     */
    @Bean(name = COFFEE_RETRY_TEMPLATE, defaultCandidate = false)
    public KafkaTemplate<Object, Object> coffeeRetryKafkaTemplate(
            @Qualifier("coffeeRetryProducerFactory") ProducerFactory<Object, Object> coffeeRetryProducerFactory) {
        return new KafkaTemplate<>(coffeeRetryProducerFactory);
    }

    /**
     * Its producer factory is a bean too, so Spring closes the producers on shutdown
     * (a KafkaTemplate only closes a factory it created itself).
     */
    @Bean(defaultCandidate = false)
    public DefaultKafkaProducerFactory<Object, Object> coffeeRetryProducerFactory(ProducerFactory<Object, Object> producerFactory) {
        var configs = new HashMap<>(producerFactory.getConfigurationProperties());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        var valueSerializer = ((DefaultKafkaProducerFactory<Object, Object>) producerFactory).getValueSerializerSupplier();
        return new DefaultKafkaProducerFactory<>(configs, null, valueSerializer);
    }

    /**
     * Share consumers take the same connection, deserializer and Schema Registry settings as the
     * normal consumers, minus the consumer-group-only settings, plus String keys.
     */
    @Bean(BARISTA_SHARE_CONTAINER_FACTORY)
    public ShareKafkaListenerContainerFactory<Object, Object> baristaShareContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var configs = new HashMap<>(kafkaConsumerFactory.getConfigurationProperties());
        SHARE_GROUP_UNSUPPORTED.forEach(configs::remove);
        configs.remove(ConsumerConfig.CLIENT_ID_CONFIG);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var factory = new ShareKafkaListenerContainerFactory<Object, Object>(new DefaultShareConsumerFactory<>(configs));
        factory.setConcurrency(2);                                // 2 baristas per app, 4 in total
        factory.getContainerProperties().setShareAckMode(ShareAckMode.MANUAL);
        return factory;
    }
}
