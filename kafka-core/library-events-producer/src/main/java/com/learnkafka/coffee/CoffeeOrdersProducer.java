package com.learnkafka.coffee;

import com.learnavro.domain.generated.Address;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnavro.domain.generated.OrderLineItem;
import com.learnavro.domain.generated.PickUp;
import com.learnavro.domain.generated.Size;
import com.learnavro.domain.generated.Store;
import com.learnkafka.domain.CoffeeOrderEvent;
import com.learnkafka.domain.CoffeeOrderRequest;
import com.learnkafka.domain.CoffeeOrderResponse;
import jakarta.annotation.PreDestroy;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.learnkafka.event.EventMetadataFactory;
import com.learnkafka.partitioner.CoffeeVipPartitioner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST endpoint that builds and publishes Avro-encoded CoffeeOrder events to Kafka.
 * Uses KafkaAvroSerializer (Confluent) to serialize and register schemas automatically
 * (subject "coffee-orders-value").
 *
 * Coffee orders are keyed by their UUID (a String), while library events use Integer keys.
 * This class therefore builds its own KafkaTemplate from Boot's producer factory with a
 * String key serializer and the CoffeeVipPartitioner (VIP customers → partition 0). Declaring a second KafkaTemplate *bean* would make Boot's
 * auto-configured one (used by LibraryEventProducer) back off.
 */
@Slf4j
@RestController
@RequestMapping("/v1/coffee-orders")
public class CoffeeOrdersProducer {

    public static final String TOPIC = "coffee-orders";

    private final KafkaTemplate<String, CoffeeOrder> kafkaTemplate;

    private final EventMetadataFactory eventMetadataFactory;

    @Autowired
    public CoffeeOrdersProducer(ProducerFactory<?, ?> producerFactory,
                                @Value("${app.coffee.vip-customers:}") String vipCustomers,
                                @Value("${spring.application.name}") String applicationName,
                                EventMetadataFactory eventMetadataFactory) {
        this(coffeeTemplate(producerFactory, vipCustomers, applicationName + "-coffee"), eventMetadataFactory);
    }

    /** For tests: inject the template directly. */
    CoffeeOrdersProducer(KafkaTemplate<String, CoffeeOrder> kafkaTemplate, EventMetadataFactory eventMetadataFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventMetadataFactory = eventMetadataFactory;
    }

    /**
     * Boot's producer settings, plus: String keys, and the VIP partitioner (only for coffee orders).
     * The VIP names reach the partitioner's configure() as a producer property. Passing the
     * overrides to KafkaTemplate makes it own the copied producer factory, so destroy() closes it.
     * The copy is not a Spring bean, so it can't name its producers "<app>-producer-N" from
     * spring.application.name like Boot's factory does: without a client.id they'd be "producer-N".
     */
    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, CoffeeOrder> coffeeTemplate(ProducerFactory<?, ?> producerFactory,
                                                                     String vipCustomers, String clientId) {
        return new KafkaTemplate<>((ProducerFactory<String, CoffeeOrder>) producerFactory, false, Map.of(
                ProducerConfig.CLIENT_ID_CONFIG, clientId,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.PARTITIONER_CLASS_CONFIG, CoffeeVipPartitioner.class,
                CoffeeVipPartitioner.VIP_CUSTOMERS_CONFIG, vipCustomers));
    }

    /** The template is not a Spring bean, so close its producers when the app stops. */
    @PreDestroy
    void close() {
        kafkaTemplate.destroy();
    }

    /** Publishes coffee order. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CoffeeOrderResponse publishCoffeeOrder(
            @RequestBody @Valid CoffeeOrderRequest request,
            @RequestHeader(name = "X-Correlation-Id", required = false) String correlationId) {
        var event = CoffeeOrderEvent.placed(eventMetadataFactory.newEvent(CoffeeOrderEvent.PLACED, correlationId), request);
        var order = buildCoffeeOrder(event);
        kafkaTemplate.send(TOPIC, order.getId().toString(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish coffee order id={}: {}", order.getId(), ex.getMessage(), ex);
                    } else {
                        log.info("Published coffee order id={} to partition={}",
                                order.getId(), result.getRecordMetadata().partition());
                    }
                });
        return CoffeeOrderResponse.from(order, event.metadata());
    }

    private CoffeeOrder buildCoffeeOrder(CoffeeOrderEvent event) {
        var address = Address.newBuilder()
                .setAddressLine1("123 Coffee Lane")
                .setCity("Seattle")
                .setStateProvince("WA")
                .setCountry("USA")
                .setZip("98101")
                .build();

        var store = Store.newBuilder()
                .setId(1)
                .setAddress(address)
                .build();

        var lineItem = OrderLineItem.newBuilder()
                .setName("Latte")
                .setSize(Size.MEDIUM)
                .setQuantity(1)
                .setCost(new BigDecimal("4.50"))
                .build();

        return CoffeeOrder.newBuilder()
                .setMetadata(event.metadata().toAvro())
                .setId(event.orderId())
                .setName(event.name())
                .setNickName(event.nickName())
                .setStore(store)
                .setOrderLineItems(List.of(lineItem))
                .setOrderedTime(Instant.now())
                .setPickUp(PickUp.IN_STORE)
                .setStatus("NEW")
                .build();
    }
}
