package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

/**
 * Queries Kafka Streams materialized state stores for order counts and revenue.
 *
 * GoF Strategy: the store name is resolved per order type, making the retrieval strategy
 * interchangeable without changing the query logic.
 *
 * Every store iterator is a native RocksDB resource and MUST be closed — {@link #collect} does
 * that with try-with-resources. Window times are UTC, matching OrderTimeStampExtractor.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    // ── Count queries ───────────────────────────────────────────────────────

    public List<OrderCountPerStoreDTO> getOrderCount(String orderType) {
        return collect(countStore(orderType(orderType)).all(), kv -> new OrderCountPerStoreDTO(kv.key, kv.value));
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {
        var result = new ArrayList<AllOrdersCountPerStoreDTO>();
        for (var type : OrderType.values()) {
            result.addAll(collect(countStore(type).all(), kv -> new AllOrdersCountPerStoreDTO(kv.key, kv.value, type)));
        }
        return result;
    }

    public OrderCountPerStoreDTO getOrderCountByLocationId(String orderType, String locationId) {
        var count = countStore(orderType(orderType)).get(locationId);
        return count != null ? new OrderCountPerStoreDTO(locationId, count) : null;
    }

    // ── Revenue queries ─────────────────────────────────────────────────────

    public List<OrderRevenueDTO> getOrderRevenue(String orderType) {
        var type = orderType(orderType);
        return collect(revenueStore(type).all(), kv -> new OrderRevenueDTO(kv.key, type, kv.value));
    }

    public OrderRevenueDTO getOrderRevenueByLocationId(String orderType, String locationId) {
        var type = orderType(orderType);
        var revenue = revenueStore(type).get(locationId);
        return revenue != null ? new OrderRevenueDTO(locationId, type, revenue) : null;
    }

    /** Revenue joined with the store's details (address, contact). Stores without a stores record are absent. */
    public List<TotalRevenueWithAddress> getOrderRevenueWithAddress(String orderType) {
        var type = orderType(orderType);
        ReadOnlyKeyValueStore<String, TotalRevenueWithAddress> store =
                store(storeName(type, OrdersTopology.GENERAL_ORDERS_REVENUE_WITH_ADDRESS,
                        OrdersTopology.RESTAURANT_ORDERS_REVENUE_WITH_ADDRESS), QueryableStoreTypes.keyValueStore());
        return collect(store.all(), kv -> kv.value);
    }

    // ── Windowed queries ────────────────────────────────────────────────────

    /**
     * Counts per store per 30 s window. With {@code from}/{@code to}, only windows whose START lies in
     * [from, to] (both inclusive); without them, every window still retained in the store.
     */
    public List<OrdersCountPerStoreByWindowsDTO> getOrderCountWindows(String orderType, LocalDateTime from, LocalDateTime to) {
        return countWindows(orderType(orderType), from, to);
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrderCountWindows(LocalDateTime from, LocalDateTime to) {
        var result = new ArrayList<OrdersCountPerStoreByWindowsDTO>();
        for (var type : OrderType.values()) {
            result.addAll(countWindows(type, from, to));
        }
        return result;
    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrderRevenueWindows(String orderType, LocalDateTime from, LocalDateTime to) {
        var type = orderType(orderType);
        ReadOnlyWindowStore<String, TotalRevenue> store = store(storeName(type,
                OrdersTopology.GENERAL_ORDERS_REVENUE_WINDOWS, OrdersTopology.RESTAURANT_ORDERS_REVENUE_WINDOWS),
                QueryableStoreTypes.windowStore());
        return collect(windowIterator(store, from, to), kv -> new OrdersRevenuePerStoreByWindowsDTO(
                kv.key.key(), kv.value, type, utc(kv.key.window().startTime()), utc(kv.key.window().endTime())));
    }

    private List<OrdersCountPerStoreByWindowsDTO> countWindows(OrderType type, LocalDateTime from, LocalDateTime to) {
        ReadOnlyWindowStore<String, Long> store = store(storeName(type,
                OrdersTopology.GENERAL_ORDERS_COUNT_WINDOWS, OrdersTopology.RESTAURANT_ORDERS_COUNT_WINDOWS),
                QueryableStoreTypes.windowStore());
        return collect(windowIterator(store, from, to), kv -> new OrdersCountPerStoreByWindowsDTO(
                kv.key.key(), kv.value, type, utc(kv.key.window().startTime()), utc(kv.key.window().endTime())));
    }

    private static <V> KeyValueIterator<Windowed<String>, V> windowIterator(
            ReadOnlyWindowStore<String, V> store, LocalDateTime from, LocalDateTime to) {
        if (from == null && to == null) {
            return store.all();
        }
        var fromInstant = from != null ? from.toInstant(ZoneOffset.UTC) : Instant.EPOCH;
        var toInstant = to != null ? to.toInstant(ZoneOffset.UTC) : Instant.now();
        if (fromInstant.isAfter(toInstant)) {
            throw new IllegalArgumentException("from_time " + from + " is after to_time " + to);
        }
        return store.fetchAll(fromInstant, toInstant);
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    /** Drains a store iterator into a list and always closes it (RocksDB iterators are native resources). */
    private static <K, V, R> List<R> collect(KeyValueIterator<K, V> iterator, Function<KeyValue<K, V>, R> mapper) {
        try (iterator) {
            var result = new ArrayList<R>();
            iterator.forEachRemaining(kv -> result.add(mapper.apply(kv)));
            return result;
        }
    }

    private ReadOnlyKeyValueStore<String, Long> countStore(OrderType type) {
        return store(storeName(type, OrdersTopology.GENERAL_ORDERS_COUNT, OrdersTopology.RESTAURANT_ORDERS_COUNT),
                QueryableStoreTypes.keyValueStore());
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> revenueStore(OrderType type) {
        return store(storeName(type, OrdersTopology.GENERAL_ORDERS_REVENUE, OrdersTopology.RESTAURANT_ORDERS_REVENUE),
                QueryableStoreTypes.keyValueStore());
    }

    /** Throws InvalidStateStoreException while the stores are not queryable (starting, rebalancing). */
    private <T> T store(String name, QueryableStoreType<T> type) {
        return kafkaStreams().store(StoreQueryParameters.fromNameAndType(name, type));
    }

    private KafkaStreams kafkaStreams() {
        var streams = streamsBuilderFactoryBean.getKafkaStreams();
        if (streams == null) {
            throw new IllegalStateException("KafkaStreams is not running");
        }
        return streams;
    }

    private static OrderType orderType(String orderType) {
        try {
            return OrderType.valueOf(orderType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown orderType: " + orderType + " (expected general or restaurant)");
        }
    }

    private static String storeName(OrderType type, String generalStore, String restaurantStore) {
        return switch (type) {
            case GENERAL -> generalStore;
            case RESTAURANT -> restaurantStore;
        };
    }

    private static LocalDateTime utc(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }
}
