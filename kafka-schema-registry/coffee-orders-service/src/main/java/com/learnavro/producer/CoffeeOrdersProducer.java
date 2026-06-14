package com.learnavro.producer;

import com.learnavro.domain.generated.Address;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnavro.domain.generated.OrderLineItem;
import com.learnavro.domain.generated.PickUp;
import com.learnavro.domain.generated.Size;
import com.learnavro.domain.generated.Store;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * REST endpoint that builds and publishes Avro-encoded CoffeeOrder events to Kafka.
 * Uses KafkaAvroSerializer (Confluent) to serialize and register schemas automatically.
 */
@RestController
@RequestMapping("/v1/coffee-orders")
@Slf4j
@RequiredArgsConstructor
public class CoffeeOrdersProducer {

    public static final String TOPIC = "coffee-orders";

    private final KafkaTemplate<String, CoffeeOrder> kafkaTemplate;

    public record CoffeeOrderRequest(
            @NotBlank String name,
            String nickName
    ) {}

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CoffeeOrder publishCoffeeOrder(@RequestBody @Valid CoffeeOrderRequest request) {
        var order = buildCoffeeOrder(request);
        kafkaTemplate.send(TOPIC, order.getId().toString(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish coffee order id={}: {}", order.getId(), ex.getMessage(), ex);
                    } else {
                        log.info("Published coffee order id={} to partition={}",
                                order.getId(), result.getRecordMetadata().partition());
                    }
                });
        return order;
    }

    private CoffeeOrder buildCoffeeOrder(CoffeeOrderRequest request) {
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
                .setId(UUID.randomUUID())
                .setName(request.name())
                .setNickName(request.nickName() != null ? request.nickName() : "")
                .setStore(store)
                .setOrderLineItems(List.of(lineItem))
                .setOrderedTime(Instant.now())
                .setPickUp(PickUp.IN_STORE)
                .setStatus("NEW")
                .build();
    }
}
