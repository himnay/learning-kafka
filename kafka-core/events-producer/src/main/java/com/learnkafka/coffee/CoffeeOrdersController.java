package com.learnkafka.coffee;

import com.learnkafka.domain.CoffeeOrderRequest;
import com.learnkafka.domain.CoffeeOrderResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint for coffee orders. A 201 means the order and its event are committed in PostgreSQL;
 * Debezium publishes the event to "coffee-orders" right after, even if Kafka was down at that moment.
 */
@Slf4j
@RestController
@RequestMapping("/v1/coffee-orders")
@RequiredArgsConstructor
public class CoffeeOrdersController {

    private final CoffeeOrderService coffeeOrderService;

    /** Places a coffee order. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CoffeeOrderResponse placeOrder(@RequestBody @Valid CoffeeOrderRequest request,
                                          @RequestHeader(name = "X-Correlation-Id", required = false) String correlationId) {
        var response = coffeeOrderService.place(request, correlationId);
        log.info("Stored coffee order id={} with its outbox event (Debezium publishes it to {})",
                response.id(), CoffeeOrderService.TOPIC);
        return response;
    }
}
