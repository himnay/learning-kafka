package com.learnkafka.coffee;

import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.event.ReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * Observer Pattern: Spring Kafka registers this listener as an observer on the "coffee-orders" topic.
 * The shared ErrorHandlingDeserializer → KafkaAvroDeserializer hands it a typed, generated CoffeeOrder
 * (specific.avro.reader=true), resolved through Schema Registry.
 *
 * Consumer-a and consumer-b both run this listener in their own shared group
 * ("coffee-orders-listener-group"), so the coffee-orders partitions are split between them
 * exactly like library-events. Coffee keys are UUID strings, hence the key.deserializer override.
 *
 * Failures go through NON-BLOCKING retry topics (@RetryableTopic, Error handling pattern 3):
 * coffee-orders-retry-0 (after 1 s), -retry-1 (2 s), -retry-2 (4 s), then coffee-orders-dlt. The main
 * topic keeps flowing meanwhile — at the cost of per-key ordering. @RetryableTopic sits on THIS method
 * only; a RetryTopicConfiguration bean would match by topic and also hijack the barista share listener.
 * To demo it, the customer name decides:
 *  - contains "flaky" → fails on the main topic only, succeeds on the first retry topic
 *  - contains "fail"  → fails every attempt and ends on coffee-orders-dlt
 */
@Slf4j
@Component
public class CoffeeOrdersConsumer {

    public static final String TOPIC = "coffee-orders";
    public static final String GROUP_ID = "coffee-orders-listener-group";

    /** Handles message. */
    @KafkaListener(topics = TOPIC, groupId = GROUP_ID,
            containerFactory = CoffeeKafkaConfig.COFFEE_ORDERS_CONTAINER_FACTORY,
            properties = "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    @RetryableTopic(
            attempts = "4",                                            // 1 main + 3 retry topics
            backOff = @BackOff(delay = 1_000, multiplier = 2.0, maxDelay = 10_000),
            include = CoffeeMachineBusyException.class,                // anything else → straight to the DLT
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            autoCreateTopics = "true", numPartitions = "3", replicationFactor = "-1",   // RF -1 = broker default
            kafkaTemplate = CoffeeKafkaConfig.COFFEE_RETRY_TEMPLATE,
            listenerContainerFactory = CoffeeKafkaConfig.COFFEE_ORDERS_CONTAINER_FACTORY)
    public void onMessage(ConsumerRecord<String, CoffeeOrder> record) {
        var order = record.value();
        var name = order.getName().toString().toLowerCase(Locale.ROOT);
        if (name.contains("flaky") && TOPIC.equals(record.topic())) {
            throw new CoffeeMachineBusyException("Coffee machine busy (first attempt) for order " + order.getId());
        }
        if (name.contains("fail")) {
            throw new CoffeeMachineBusyException("Coffee machine broken for order " + order.getId());
        }
        var received = ReceivedEvent.of(order);   // child event + common metadata
        log.info("Received CoffeeOrder: id={} name='{}' status={} store={} topic={} partition={} eventId={}",
                order.getId(), order.getName(), order.getStatus(), order.getStore().getId(),
                record.topic(), record.partition(), received.eventId());
    }

    /** Transient failure: worth retrying later without blocking the partition. */
    public static class CoffeeMachineBusyException extends RuntimeException {
        public CoffeeMachineBusyException(String message) {
            super(message);
        }
    }
}
