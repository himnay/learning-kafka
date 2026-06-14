package com.learnavro.consumer;

import com.learnavro.domain.generated.CoffeeOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Observer Pattern: Spring Kafka registers this listener as an observer on the "coffee-orders" topic.
 * Confluent KafkaAvroDeserializer handles Avro deserialization and schema registry lookup.
 */
@Component
@Slf4j
public class CoffeeOrdersConsumer {

    @KafkaListener(topics = "coffee-orders", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(ConsumerRecord<String, CoffeeOrder> record) {
        var order = record.value();
        log.info("Received CoffeeOrder: id={} name='{}' status={} store={}",
                order.getId(), order.getName(), order.getStatus(), order.getStore().getId());
    }
}
