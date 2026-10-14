package com.learnkafka.coffee;

import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.event.EventMetadataMdcInterceptor;
import com.learnkafka.event.ReceivedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * SHARE GROUP (KIP-932, "Queues for Kafka", GA in Kafka 4.2): a work queue on the same coffee-orders
 * topic. Every barista thread in consumer-a and consumer-b belongs to share group "coffee-baristas".
 * Unlike a consumer group, records are handed out ONE BY ONE to any member — partitions are not
 * owned, so the number of baristas is not capped by the partition count (and there is no ordering).
 *
 * Each record is acknowledged individually (ShareAckMode.MANUAL):
 *  - acknowledge() = ACCEPT  → done
 *  - release()     = RELEASE → back to the queue, redelivered (maybe to another barista), delivery count + 1
 *  - reject()      = REJECT  → poison, never redelivered (used for records that are not valid Avro)
 * Demo: a customer name starting with "busy" is released on its first delivery and accepted on the next.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "app.coffee.share-group.enabled", havingValue = "true", matchIfMissing = true)
public class BaristaShareConsumer {

    public static final String SHARE_GROUP_ID = "coffee-baristas";

    @KafkaListener(topics = CoffeeOrdersConsumer.TOPIC, groupId = SHARE_GROUP_ID,
            containerFactory = CoffeeKafkaConfig.BARISTA_SHARE_CONTAINER_FACTORY)
    public void onMessage(ConsumerRecord<String, CoffeeOrder> record, ShareAcknowledgment acknowledgment) {
        var order = record.value();
        if (order == null) {
            // ErrorHandlingDeserializer hands over null when the bytes are not valid Avro: a poison record
            log.warn("Barista REJECT undeserializable record partition={} offset={}", record.partition(), record.offset());
            acknowledgment.reject();
            return;
        }
        // share containers have no RecordInterceptor: put the base-event metadata into the MDC here
        EventMetadataMdcInterceptor.put(ReceivedEvent.of(order).metadata());
        try {
            makeOrder(record, order, acknowledgment);
        } finally {
            EventMetadataMdcInterceptor.clear();
        }
    }

    private void makeOrder(ConsumerRecord<String, CoffeeOrder> record, CoffeeOrder order, ShareAcknowledgment acknowledgment) {
        short deliveryCount = record.deliveryCount().orElse((short) 1);
        if (order.getName().toString().toLowerCase(Locale.ROOT).startsWith("busy") && deliveryCount == 1) {
            log.info("Barista busy — RELEASE order id={} (delivery {}) partition={}",
                    order.getId(), deliveryCount, record.partition());
            acknowledgment.release();
            return;
        }
        log.info("Barista made order id={} name='{}' (delivery {}) partition={}",
                order.getId(), order.getName(), deliveryCount, record.partition());
        acknowledgment.acknowledge();
    }
}
