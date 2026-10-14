package com.learnkafka.domain;

import jakarta.validation.constraints.NotBlank;

/** REST input for POST /v1/coffee-orders. The producer builds the full Avro CoffeeOrder from it. */
public record CoffeeOrderRequest(
        @NotBlank String name,
        String nickName
) {}
