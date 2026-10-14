package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Address;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.service.OrderStoreService;
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
import org.junit.jupiter.api.DisplayName;
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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES}, partitions = 1)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.application-id=orders-integration-test",
        // fresh RocksDB state per test context: every context (DirtiesContext) gets a new embedded broker
        "spring.kafka.streams.state-dir=${java.io.tmpdir}/orders-streams-it-${random.uuid}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderStoreService orderStoreService;

    @BeforeEach
    void waitForStreamsRunning() {
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> {
            var streams = streamsBuilderFactoryBean.getKafkaStreams();
            return streams != null && streams.state() == KafkaStreams.State.RUNNING;
        });
    }

    @Test
    @DisplayName("Publishing general orders through the streams topology materialises the correct order count and running revenue per store")
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

    @Test
    @DisplayName("Windowed counts and revenue joined with the store are queryable through OrderStoreService")
    void windowsAndJoin_queryableThroughService() {
        publishStores();
        publishOrders();

        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    // order time 2023-02-21T21:25:01 → window [21:25:00, 21:25:30)
                    assertThat(orderStoreService.getOrderCountWindows("general", null, null))
                            .singleElement()
                            .satisfies(w -> {
                                assertEquals("store_1234", w.locationId());
                                assertEquals(1L, w.orderCount());
                                assertEquals(LocalDateTime.parse("2023-02-21T21:25:00"), w.startWindow());
                                assertEquals(LocalDateTime.parse("2023-02-21T21:25:30"), w.endWindow());
                            });
                    assertThat(orderStoreService.getOrderCountWindows("general",
                            LocalDateTime.parse("2023-02-21T21:26:00"), LocalDateTime.parse("2023-02-21T21:30:00")))
                            .isEmpty();

                    assertThat(orderStoreService.getOrderRevenueWithAddress("restaurant"))
                            .singleElement()
                            .satisfies(j -> {
                                assertEquals(new BigDecimal("15.00"), j.totalRevenue().runningRevenue());
                                assertEquals("City1", j.store().address().city());
                            });
                });
    }

    private void publishStores() {
        var store = new Store("store_1234", new Address("1234 Street 1", "", "City1", "State1", "12345"), "1234567890");
        try {
            kafkaTemplate.send(STORES, store.locationId(), objectMapper.writeValueAsString(store));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
