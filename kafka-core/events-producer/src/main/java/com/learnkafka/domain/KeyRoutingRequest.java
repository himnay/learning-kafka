package com.learnkafka.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** REST input for POST /v1/key-routing. The key decides the partition (KeySubstringPartitioner). */
public record KeyRoutingRequest(
        @NotBlank String key,
        @NotNull String message
) {}
