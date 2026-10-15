package com.learnkafka.domain;

import com.learnkafka.event.BaseEvent;
import com.learnkafka.event.EventMetadata;

import java.util.UUID;

/** Child event of {@link BaseEvent}: a coffee order was placed. Mapped to the Avro CoffeeOrder. */
public record CoffeeOrderEvent(EventMetadata metadata, UUID orderId, String name, String nickName)
        implements BaseEvent {

    public static final String PLACED = "CoffeeOrder.PLACED";

    public static CoffeeOrderEvent placed(EventMetadata metadata, CoffeeOrderRequest request) {
        return new CoffeeOrderEvent(metadata, UUID.randomUUID(), request.name(),
                request.nickName() != null ? request.nickName() : "");
    }
}
