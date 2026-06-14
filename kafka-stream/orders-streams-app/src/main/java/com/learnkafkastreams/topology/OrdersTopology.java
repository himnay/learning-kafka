package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams topology for order count and revenue aggregation.
 *
 * GoF patterns:
 *  - Template Method: orderTopology() defines the processing skeleton; aggregation
 *    steps are extracted into aggregateOrdersCountAndRevenue() per order type.
 *  - Strategy: branching logic (GENERAL vs RESTAURANT) is a pluggable predicate.
 */
@Component
@Slf4j
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

    // Spring Kafka Streams requires @Autowired method injection to register the topology
    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        orderTopology(streamsBuilder);
    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {

        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor()))
                .selectKey((key, order) -> order.locationId());

        var storesTable = streamsBuilder
                .table(STORES, Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

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

        aggregateOrdersCountAndRevenue(
                branches.get("split-general"),
                GENERAL_ORDERS_COUNT,
                GENERAL_ORDERS_REVENUE,
                storesTable);

        aggregateOrdersCountAndRevenue(
                branches.get("split-restaurant"),
                RESTAURANT_ORDERS_COUNT,
                RESTAURANT_ORDERS_REVENUE,
                storesTable);
    }

    /**
     * Aggregates order count and total revenue per store (by locationId) and
     * materialises both into named state stores for interactive queries.
     */
    private static void aggregateOrdersCountAndRevenue(
            KStream<String, Order> orderStream,
            String countStoreName,
            String revenueStoreName,
            KTable<String, Store> storesTable) {

        var grouped = orderStream
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)));

        // ── Count per store ─────────────────────────────────────────────────
        grouped
                .count(Materialized
                        .<String, Long, KeyValueStore<Bytes, byte[]>>as(countStoreName)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((key, count) -> log.debug("[{}] locationId={} count={}", countStoreName, key, count));

        // ── Revenue aggregation per store ───────────────────────────────────
        grouped
                .aggregate(
                        TotalRevenue::new,
                        (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order),
                        Materialized
                                .<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(revenueStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class)))
                .toStream()
                .peek((key, revenue) -> log.debug("[{}] locationId={} revenue={}", revenueStoreName, key, revenue));
    }
}
