package com.learnkafka.coffee;

import com.learnavro.domain.generated.CoffeeOrder;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Stores the order itself (coffee.coffee_order), in the caller's transaction. */
@Repository
@RequiredArgsConstructor
public class CoffeeOrderRepository {

    private final JdbcClient jdbcClient;

    public void insert(CoffeeOrder order) {
        jdbcClient.sql("""
                        INSERT INTO coffee.coffee_order (id, name, nick_name, store_id, status, pick_up, ordered_time)
                        VALUES (:id, :name, :nickName, :storeId, :status, :pickUp, :orderedTime)
                        """)
                .param("id", order.getId())
                .param("name", order.getName().toString())
                .param("nickName", order.getNickName().toString())
                .param("storeId", order.getStore().getId())
                .param("status", order.getStatus().toString())
                .param("pickUp", order.getPickUp().name())
                .param("orderedTime", OffsetDateTime.ofInstant(order.getOrderedTime(), ZoneOffset.UTC))
                .update();
    }
}
