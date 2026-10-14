package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Address;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.assertj.core.api.Assertions.assertThat;
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
                new JacksonJsonSerde<>(Order.class).serializer());

        storesInputTopic = topologyTestDriver.createInputTopic(
                STORES,
                Serdes.String().serializer(),
                new JacksonJsonSerde<>(Store.class).serializer());
    }

    @AfterEach
    void tearDown() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    @Test
    @DisplayName("Topology counts general orders per store correctly in the count store")
    void countGeneralOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var countStore = topologyTestDriver.<String, Long>getKeyValueStore(GENERAL_ORDERS_COUNT);
        assertEquals(1L, countStore.get("store_1234"));
        assertEquals(1L, countStore.get("store_4567"));
    }

    @Test
    @DisplayName("Topology counts restaurant orders per store correctly in the count store")
    void countRestaurantOrdersByStore() {
        storesInputTopic.pipeKeyValueList(buildStores());
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var countStore = topologyTestDriver.<String, Long>getKeyValueStore(RESTAURANT_ORDERS_COUNT);
        assertEquals(1L, countStore.get("store_1234"));
    }

    @Test
    @DisplayName("Topology computes running revenue and order count for general orders per store")
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
    @DisplayName("Topology computes running revenue and order count for restaurant orders per store")
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
    @DisplayName("Multiple orders for the same store accumulate count and revenue rather than overwriting it")
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

    @Test
    @DisplayName("Windowed count: orders are counted per store per 30 s tumbling window, by order time")
    void windowedCount_perThirtySecondWindow() {
        var items = List.of(new OrderLineItem("Bananas", 1, new BigDecimal("5.00")));
        var start = LocalDateTime.parse("2026-02-16T11:27:00");
        // two orders in [11:27:00, 11:27:30), one in [11:27:30, 11:28:00)
        ordersInputTopic.pipeInput("1", new Order(1, "store_1234", new BigDecimal("5.00"), OrderType.GENERAL, items, start.plusSeconds(1)));
        ordersInputTopic.pipeInput("2", new Order(2, "store_1234", new BigDecimal("5.00"), OrderType.GENERAL, items, start.plusSeconds(29)));
        ordersInputTopic.pipeInput("3", new Order(3, "store_1234", new BigDecimal("5.00"), OrderType.GENERAL, items, start.plusSeconds(31)));

        var windowStore = topologyTestDriver.<String, Long>getWindowStore(GENERAL_ORDERS_COUNT_WINDOWS);
        try (var windows = windowStore.fetch("store_1234", utc(start), utc(start.plusMinutes(1)))) {
            var counts = new java.util.LinkedHashMap<LocalDateTime, Long>();
            windows.forEachRemaining(kv -> counts.put(LocalDateTime.ofInstant(Instant.ofEpochMilli(kv.key), ZoneOffset.UTC), kv.value));
            assertThat(counts).containsExactly(
                    Map.entry(start, 2L),
                    Map.entry(start.plusSeconds(30), 1L));
        }
    }

    @Test
    @DisplayName("Window retention: windows stay queryable for an hour of stream time, not just size + grace")
    void windows_retainedForAnHour() {
        var items = List.of(new OrderLineItem("Bananas", 1, new BigDecimal("5.00")));
        var start = LocalDateTime.parse("2026-02-16T11:27:00");
        ordersInputTopic.pipeInput("1", new Order(1, "store_1234", new BigDecimal("5.00"), OrderType.GENERAL, items, start.plusSeconds(1)));
        // advance stream time by 30 minutes: far beyond size + grace (40 s), within the 1 h retention
        ordersInputTopic.pipeInput("2", new Order(2, "store_1234", new BigDecimal("5.00"), OrderType.GENERAL, items, start.plusMinutes(30)));

        var windowStore = topologyTestDriver.<String, Long>getWindowStore(GENERAL_ORDERS_COUNT_WINDOWS);
        assertEquals(1L, windowStore.fetch("store_1234", utc(start).toEpochMilli()));
    }

    @Test
    @DisplayName("Windowed revenue: running revenue is aggregated per store per window")
    void windowedRevenue_perWindow() {
        var items = List.of(new OrderLineItem("Pizza", 1, new BigDecimal("12.00")));
        var start = LocalDateTime.parse("2026-02-16T11:27:00");
        ordersInputTopic.pipeInput("1", new Order(1, "store_1234", new BigDecimal("12.00"), OrderType.RESTAURANT, items, start.plusSeconds(5)));
        ordersInputTopic.pipeInput("2", new Order(2, "store_1234", new BigDecimal("3.00"), OrderType.RESTAURANT, items, start.plusSeconds(10)));

        var windowStore = topologyTestDriver.<String, TotalRevenue>getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
        var revenue = windowStore.fetch("store_1234", utc(start).toEpochMilli());
        assertNotNull(revenue);
        assertEquals(new BigDecimal("15.00"), revenue.runningRevenue());
        assertEquals(2, revenue.runningOrderCount());
    }

    @Test
    @DisplayName("Join: revenue is joined with the store's details; a store without a stores record has no joined row")
    void revenueJoinedWithStore() {
        storesInputTopic.pipeInput("store_1234", buildStores().get(0).value);   // store_4567 deliberately missing
        ordersInputTopic.pipeKeyValueList(buildOrders());

        var joined = topologyTestDriver.<String, TotalRevenueWithAddress>getKeyValueStore(GENERAL_ORDERS_REVENUE_WITH_ADDRESS);
        var row = joined.get("store_1234");
        assertNotNull(row);
        assertEquals(new BigDecimal("27.00"), row.totalRevenue().runningRevenue());
        assertEquals("City1", row.store().address().city());
        assertThat(joined.get("store_4567")).isNull();   // inner join: no store record → no row
    }

    private static Instant utc(LocalDateTime time) {
        return time.toInstant(ZoneOffset.UTC);
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
