package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Address;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class OrdersTopologyTest {

    TopologyTestDriver topologyTestDriver;
    TestInputTopic<String, Order> ordersInputTopic;
    TestInputTopic<String, Store> storesInputTopic;

    @BeforeEach
    void setUp() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        var streamsBuilder = new StreamsBuilder();
        new OrdersTopology().process(streamsBuilder);
        topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);

        ordersInputTopic = topologyTestDriver.createInputTopic(
                ORDERS,
                Serdes.String().serializer(),
                new JsonSerde<>(Order.class).serializer());

        storesInputTopic = topologyTestDriver.createInputTopic(
                STORES,
                Serdes.String().serializer(),
                new JsonSerde<>(Store.class).serializer());
    }

    @AfterEach
    void tearDown() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    @Test
    void countGeneralOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var countStore = topologyTestDriver.<String, Long>getKeyValueStore(GENERAL_ORDERS_COUNT);
        assertEquals(1L, countStore.get("store_1234"));
        assertEquals(1L, countStore.get("store_4567"));
    }

    @Test
    void countRestaurantOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var countStore = topologyTestDriver.<String, Long>getKeyValueStore(RESTAURANT_ORDERS_COUNT);
        assertEquals(1L, countStore.get("store_1234"));
    }

    @Test
    void revenueGeneralOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var revenueStore = topologyTestDriver.<String, TotalRevenue>getKeyValueStore(GENERAL_ORDERS_REVENUE);
        var revenue = revenueStore.get("store_1234");
        assertNotNull(revenue);
        assertEquals(new BigDecimal("27.00"), revenue.runningRevenue());
        assertEquals(1, revenue.runningOrderCount());
    }

    @Test
    void revenueRestaurantOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var revenueStore = topologyTestDriver.<String, TotalRevenue>getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
        var revenue = revenueStore.get("store_1234");
        assertNotNull(revenue);
        assertEquals(new BigDecimal("15.00"), revenue.runningRevenue());
        assertEquals(1, revenue.runningOrderCount());
    }

    @Test
    void multipleOrdersSameStore_accumulatesCount() {
        storesInputTopic.pipeKeyValueList(buildStores());

        var generalItems = List.of(new OrderLineItem("Bananas", 1, new BigDecimal("5.00")));
        var extraOrder = new Order(99999, "store_1234", new BigDecimal("5.00"),
                OrderType.GENERAL, generalItems, LocalDateTime.now());

        ordersInputTopic.pipeKeyValueList(buildOrders());
        ordersInputTopic.pipeInput("99999", extraOrder);

        var countStore = topologyTestDriver.<String, Long>getKeyValueStore(GENERAL_ORDERS_COUNT);
        assertEquals(2L, countStore.get("store_1234"));

        var revenueStore = topologyTestDriver.<String, TotalRevenue>getKeyValueStore(GENERAL_ORDERS_REVENUE);
        assertEquals(new BigDecimal("32.00"), revenueStore.get("store_1234").runningRevenue());
    }

    static List<KeyValue<String, Order>> buildOrders() {
        var generalItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );
        var restaurantItems = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234", new BigDecimal("27.00"),
                OrderType.GENERAL, generalItems, LocalDateTime.now());
        var order2 = new Order(54321, "store_1234", new BigDecimal("15.00"),
                OrderType.RESTAURANT, restaurantItems, LocalDateTime.now());
        var order3 = new Order(12345, "store_4567", new BigDecimal("27.00"),
                OrderType.GENERAL, generalItems, LocalDateTime.now());

        return List.of(
                KeyValue.pair(order1.orderId().toString(), order1),
                KeyValue.pair(order2.orderId().toString(), order2),
                KeyValue.pair(order3.orderId().toString(), order3)
        );
    }

    static List<KeyValue<String, Store>> buildStores() {
        return List.of(
                KeyValue.pair("store_1234",
                        new Store("store_1234",
                                new Address("1234 Street 1", "", "City1", "State1", "12345"),
                                "1234567890")),
                KeyValue.pair("store_4567",
                        new Store("store_4567",
                                new Address("1234 Street 2", "", "City2", "State2", "541321"),
                                "0987654321"))
        );
    }
}
