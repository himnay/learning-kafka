package com.learnkafka.config;

import com.learnkafka.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Kafka consumer container configuration.
 *
 * GoF patterns applied:
 *   - Strategy      : pluggable ConsumerRecordRecoverer injected into DefaultErrorHandler
 *   - Template Method : recoverer branches on exception type (recoverable vs non-recoverable)
 *   - Factory Method  : @Bean creates the ConcurrentKafkaListenerContainerFactory
 */
@Configuration
@EnableKafka
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

    static final String DLT_TOPIC = "library-events.DLT";

    private final LibraryEventsService libraryEventsService;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    /** 3 concurrent listener threads — matches the 3 topic partitions. */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.setConcurrency(3);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

    /**
     * DefaultErrorHandler replaces the deprecated RetryTemplate / setErrorHandler APIs.
     * - FixedBackOff: 1 s delay, 2 retries (3 total attempts)
     * - IllegalArgumentException is not retried — skips to recoverer immediately
     */
    private CommonErrorHandler errorHandler() {
        var backOff  = new FixedBackOff(1_000L, 2);
        var handler  = new DefaultErrorHandler(recoverer(), backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record {}", deliveryAttempt, record, ex));
        return handler;
    }

    /**
     * Strategy pattern: recovery behaviour selected by root exception type.
     *  - RecoverableDataAccessException → re-publish the event for later reprocessing
     *  - All other exceptions            → route to DLT via DeadLetterPublishingRecoverer
     */
    @SuppressWarnings("unchecked")
    private ConsumerRecordRecoverer recoverer() {
        var dltRecoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Routing to DLT: key={} cause={}", record.key(), ex.getMessage());
                    return new TopicPartition(DLT_TOPIC, record.partition());
                });

        return (record, ex) -> {
            var cause = ex.getCause() != null ? ex.getCause() : ex;
            if (cause instanceof RecoverableDataAccessException) {
                log.info("Recoverable exception — re-publishing record: key={}", record.key());
                libraryEventsService.handleRecovery((ConsumerRecord<Integer, String>) record)
                        .whenComplete((result, throwable) -> {
                            if (throwable != null) {
                                log.error("Recovery re-publish failed: key={}", record.key(), throwable);
                            }
                        });
            } else {
                log.error("Non-recoverable exception — routing to DLT: key={}", record.key(), ex);
                dltRecoverer.accept(record, ex);
            }
        };
    }

    @Bean
    public NewTopic libraryEventsDlt() {
        return TopicBuilder.name(DLT_TOPIC).partitions(3).replicas(1).build();
    }
}
