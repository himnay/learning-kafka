package com.learnkafka.domain;

import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.event.EventMetadata;

import java.time.Instant;

/**
 * JSON view of the published order. Avro {@code SpecificRecord}s are not Jackson-friendly —
 * serializing one walks {@code getSchema()} and fails ("Not an array: {...}").
 */
public record CoffeeOrderResponse(String id, String name, String nickName, String status, Instant orderedTime,
                                  EventMetadata metadata) {

    public static CoffeeOrderResponse from(CoffeeOrder order, EventMetadata metadata) {
        return new CoffeeOrderResponse(order.getId().toString(), order.getName().toString(),
                order.getNickName().toString(), order.getStatus().toString(), order.getOrderedTime(), metadata);
    }
}
