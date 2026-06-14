package com.learnkafkastreams.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

/**
 * Manual test utility — run main() to seed the local Kafka cluster with sample orders.
 * Not part of the automated test suite; kept in src/test to avoid production classpath pollution.
 */
@Slf4j
public class OrdersMockDataProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) {
        publishOrders(buildOrders());
    }

    static List<Order> buildOrders() {
        var generalItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );
        var restaurantItems = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        return List.of(
                new Order(12345, "store_1234", new BigDecimal("27.00"), OrderType.GENERAL, generalItems, LocalDateTime.now()),
                new Order(54321, "store_1234", new BigDecimal("15.00"), OrderType.RESTAURANT, restaurantItems, LocalDateTime.now()),
                new Order(12345, "store_4567", new BigDecimal("27.00"), OrderType.GENERAL, generalItems, LocalDateTime.now()),
                new Order(12345, "store_4567", new BigDecimal("27.00"), OrderType.RESTAURANT, generalItems, LocalDateTime.now())
        );
    }

    static void publishOrders(List<Order> orders) {
        orders.forEach(order -> {
            try {
                var json = OBJECT_MAPPER.writeValueAsString(order);
                var metadata = publishMessageSync(OrdersTopology.ORDERS, order.orderId() + "", json);
                log.info("Published order: {}", metadata);
            } catch (Exception e) {
                log.error("Failed to publish order: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }
}
