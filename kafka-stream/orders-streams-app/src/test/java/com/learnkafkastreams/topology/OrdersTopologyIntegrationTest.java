package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;
import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_REVENUE;
import static com.learnkafkastreams.topology.OrdersTopology.ORDERS;
import static com.learnkafkastreams.topology.OrdersTopology.STORES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES}, partitions = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.application-id=orders-integration-test",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void waitForStreamsRunning() throws InterruptedException {
        KafkaStreams streams;
        do {
            streams = streamsBuilderFactoryBean.getKafkaStreams();
            if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
                Thread.sleep(100);
            }
        } while (streams == null || streams.state() != KafkaStreams.State.RUNNING);
    }

    @Test
    void publishGeneralOrders_countsAndRevenueMaterialised() {
        publishOrders();

        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    ReadOnlyKeyValueStore<String, Long> countStore = streamsBuilderFactoryBean.getKafkaStreams()
                            .store(StoreQueryParameters.fromNameAndType(
                                    GENERAL_ORDERS_COUNT, QueryableStoreTypes.keyValueStore()));
                    assertEquals(1L, countStore.get("store_1234"));

                    ReadOnlyKeyValueStore<String, TotalRevenue> revenueStore = streamsBuilderFactoryBean.getKafkaStreams()
                            .store(StoreQueryParameters.fromNameAndType(
                                    GENERAL_ORDERS_REVENUE, QueryableStoreTypes.keyValueStore()));
                    TotalRevenue revenue = revenueStore.get("store_1234");
                    assertNotNull(revenue);
                    assertEquals(new BigDecimal("27.00"), revenue.runningRevenue());
                    assertEquals(1, revenue.runningOrderCount());
                });
    }

    private void publishOrders() {
        orders().forEach(order -> {
            try {
                kafkaTemplate.send(ORDERS, order.key, objectMapper.writeValueAsString(order.value));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    static List<KeyValue<String, Order>> orders() {
        var generalItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );
        var restaurantItems = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234", new BigDecimal("27.00"),
                OrderType.GENERAL, generalItems, LocalDateTime.parse("2023-02-21T21:25:01"));
        var order2 = new Order(54321, "store_1234", new BigDecimal("15.00"),
                OrderType.RESTAURANT, restaurantItems, LocalDateTime.parse("2023-02-21T21:25:01"));

        return List.of(
                KeyValue.pair(order1.orderId().toString(), order1),
                KeyValue.pair(order2.orderId().toString(), order2)
        );
    }
}
