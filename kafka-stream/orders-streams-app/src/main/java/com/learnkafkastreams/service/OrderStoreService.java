package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Queries Kafka Streams materialized state stores for order counts and revenue.
 *
 * GoF Strategy: countStoreName / revenueStoreName resolve the correct store per order type,
 * making the retrieval strategy interchangeable without changing the query logic.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    // ── Count queries ───────────────────────────────────────────────────────

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        var store = getCountStore(orderType);
        var type  = OrderType.valueOf(orderType.toUpperCase());
        return toStream(store).map(kv -> new OrderCountPerStoreDTO(kv.key, kv.value)).toList();
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
        return Stream.of(OrderType.values())
                .flatMap(type -> {
                    var store = getCountStore(type.name());
                    return toStream(store).map(kv ->
                            new AllOrdersCountPerStoreDTO(kv.key, kv.value, type));
                })
                .toList();
    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId) {
        var count = getCountStore(orderType).get(locationId);
        return count != null ? new OrderCountPerStoreDTO(locationId, count) : null;
    }

    // ── Revenue queries ─────────────────────────────────────────────────────

    public List<OrderRevenueDTO> getOrderRevenue(String orderType) {
        var store = getRevenueStore(orderType);
        var type  = OrderType.valueOf(orderType.toUpperCase());
        return toStream(store).map(kv -> new OrderRevenueDTO(kv.key, type, kv.value)).toList();
    }

    public OrderRevenueDTO getOrderRevenueByLocationId(String orderType, String locationId) {
        var type    = OrderType.valueOf(orderType.toUpperCase());
        var revenue = getRevenueStore(orderType).get(locationId);
        return revenue != null ? new OrderRevenueDTO(locationId, type, revenue) : null;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private ReadOnlyKeyValueStore<String, Long> getCountStore(String orderType) {
        return kafkaStreams().store(StoreQueryParameters.fromNameAndType(
                countStoreName(orderType), QueryableStoreTypes.keyValueStore()));
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
        return kafkaStreams().store(StoreQueryParameters.fromNameAndType(
                revenueStoreName(orderType), QueryableStoreTypes.keyValueStore()));
    }

    private KafkaStreams kafkaStreams() {
        var streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) throw new IllegalStateException("KafkaStreams is not running");
        return streams;
    }

    private static <K, V> Stream<org.apache.kafka.streams.KeyValue<K, V>> toStream(
            ReadOnlyKeyValueStore<K, V> store) {
        var iterator = store.all();
        var spliterator = java.util.Spliterators.spliteratorUnknownSize(iterator,
                java.util.Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false)
                .onClose(iterator::close);
    }

    private static String countStoreName(String orderType) {
        return switch (orderType.toUpperCase()) {
            case "GENERAL"    -> OrdersTopology.GENERAL_ORDERS_COUNT;
            case "RESTAURANT" -> OrdersTopology.RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalArgumentException("Unknown orderType: " + orderType);
        };
    }

    private static String revenueStoreName(String orderType) {
        return switch (orderType.toUpperCase()) {
            case "GENERAL"    -> OrdersTopology.GENERAL_ORDERS_REVENUE;
            case "RESTAURANT" -> OrdersTopology.RESTAURANT_ORDERS_REVENUE;
            default -> throw new IllegalArgumentException("Unknown orderType: " + orderType);
        };
    }
}
