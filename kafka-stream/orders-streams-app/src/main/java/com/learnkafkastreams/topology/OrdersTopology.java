package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka Streams topology for order count and revenue aggregation.
 *
 * GoF patterns:
 *  - Template Method: orderTopology() defines the processing skeleton; aggregation
 *    steps (count, revenue, windowed count/revenue, revenue ⋈ store) are extracted into
 *    aggregateOrdersCountAndRevenue() per order type.
 *  - Strategy: branching logic (GENERAL vs RESTAURANT) is a pluggable predicate.
 */
@Slf4j
@Component
public class OrdersTopology {

    public static final String ORDERS  = "orders";
    public static final String STORES  = "stores";

    public static final String GENERAL_ORDERS                  = "general_orders";
    public static final String GENERAL_ORDERS_COUNT            = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS    = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE          = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS  = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS                  = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT            = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE          = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS    = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS  = "restaurant_orders_revenue_window";

    /** Revenue joined with the store's details (KTable-KTable join with the stores table). */
    public static final String GENERAL_ORDERS_REVENUE_WITH_ADDRESS    = "general_orders_revenue_with_address";
    public static final String RESTAURANT_ORDERS_REVENUE_WITH_ADDRESS = "restaurant_orders_revenue_with_address";

    /**
     * Tumbling windows of 30 s (event time, from OrderTimeStampExtractor). An order that arrives up to
     * 10 s after its window ended still counts (grace); later ones are dropped for that window.
     */
    public static final Duration WINDOW_SIZE  = Duration.ofSeconds(30);
    public static final Duration WINDOW_GRACE = Duration.ofSeconds(10);

    /**
     * How long window stores keep windows (in stream time). Without it Kafka Streams keeps only
     * size + grace = 40 s, so the windows endpoints could show just the last window or two.
     */
    public static final Duration WINDOW_RETENTION = Duration.ofHours(1);

    // Spring Kafka Streams requires @Autowired method injection to register the topology
    /** Processes. */
    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        orderTopology(streamsBuilder);
    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JacksonJsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor()))
                .selectKey((key, order) -> order.locationId());

        var storesTable = streamsBuilder
                .table(STORES, Consumed.with(Serdes.String(), new JacksonJsonSerde<>(Store.class)));

        storesTable.toStream()
                .peek((key, store) -> log.debug("Store: key={} value={}", key, store));

        // Branch by order type into GENERAL and RESTAURANT streams
        var branches = orderStreams
                .split(Named.as("split-"))
                .branch((key, order) -> order.orderType() == OrderType.GENERAL,
                        Branched.as("general"))
                .branch((key, order) -> order.orderType() == OrderType.RESTAURANT,
                        Branched.as("restaurant"))
                .noDefaultBranch();

        aggregateOrdersCountAndRevenue(branches.get("split-general"), storesTable,
                GENERAL_ORDERS_COUNT, GENERAL_ORDERS_REVENUE,
                GENERAL_ORDERS_COUNT_WINDOWS, GENERAL_ORDERS_REVENUE_WINDOWS, GENERAL_ORDERS_REVENUE_WITH_ADDRESS);

        aggregateOrdersCountAndRevenue(branches.get("split-restaurant"), storesTable,
                RESTAURANT_ORDERS_COUNT, RESTAURANT_ORDERS_REVENUE,
                RESTAURANT_ORDERS_COUNT_WINDOWS, RESTAURANT_ORDERS_REVENUE_WINDOWS, RESTAURANT_ORDERS_REVENUE_WITH_ADDRESS);
    }

    /**
     * Aggregates order count and total revenue per store (by locationId) — all-time and per
     * 30 s window — joins the revenue with the store's details, and materialises everything into
     * named state stores for interactive queries.
     */
    private static void aggregateOrdersCountAndRevenue(
            KStream<String, Order> orderStream,
            KTable<String, Store> storesTable,
            String countStoreName,
            String revenueStoreName,
            String countWindowsStoreName,
            String revenueWindowsStoreName,
            String revenueWithAddressStoreName) {

        var grouped = orderStream
                .groupByKey(Grouped.with(Serdes.String(), new JacksonJsonSerde<>(Order.class)));

        // ── Count per store ─────────────────────────────────────────────────
        grouped
                .count(Materialized
                        .<String, Long, KeyValueStore<Bytes, byte[]>>as(countStoreName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((key, count) -> log.debug("[{}] locationId={} count={}", countStoreName, key, count));

        // ── Revenue aggregation per store ───────────────────────────────────
        var revenueTable = grouped
                .aggregate(
                        TotalRevenue::new,
                        (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order),
                        Materialized
                                .<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(revenueStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JacksonJsonSerde<>(TotalRevenue.class)));
        revenueTable
                .toStream()
                .peek((key, revenue) -> log.debug("[{}] locationId={} revenue={}", revenueStoreName, key, revenue));

        // ── Revenue ⋈ store (KTable-KTable inner join on locationId) ──────────
        // Both tables are keyed by locationId and co-partitioned (same partition count, same
        // key hashing), so the join happens locally. A store without a stores record has no row.
        revenueTable
                .join(storesTable, TotalRevenueWithAddress::new,
                        Materialized
                                .<String, TotalRevenueWithAddress, KeyValueStore<Bytes, byte[]>>as(revenueWithAddressStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JacksonJsonSerde<>(TotalRevenueWithAddress.class)))
                .toStream()
                .peek((key, value) -> log.debug("[{}] locationId={} {}", revenueWithAddressStoreName, key, value));

        // ── Windowed: per store, per 30 s tumbling window ──────────────────────
        var windowed = grouped.windowedBy(TimeWindows.ofSizeAndGrace(WINDOW_SIZE, WINDOW_GRACE));

        windowed
                .count(Materialized
                        .<String, Long, WindowStore<Bytes, byte[]>>as(countWindowsStoreName)
                        .withRetention(WINDOW_RETENTION)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((window, count) -> log.debug("[{}] {} count={}", countWindowsStoreName, window, count));

        windowed
                .aggregate(
                        TotalRevenue::new,
                        (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order),
                        Materialized
                                .<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(revenueWindowsStoreName)
                                .withRetention(WINDOW_RETENTION)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JacksonJsonSerde<>(TotalRevenue.class)))
                .toStream()
                .peek((window, revenue) -> log.debug("[{}] {} revenue={}", revenueWindowsStoreName, window, revenue));
    }
}
