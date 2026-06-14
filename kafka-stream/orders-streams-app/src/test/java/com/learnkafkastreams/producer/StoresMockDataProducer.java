package com.learnkafkastreams.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Address;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

/**
 * Manual test utility — run main() to seed the local Kafka cluster with sample stores.
 * Not part of the automated test suite; kept in src/test to avoid production classpath pollution.
 */
@Slf4j
public class StoresMockDataProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) {
        publishStores(buildStores());
    }

    static List<Store> buildStores() {
        return List.of(
                new Store("store_1234", new Address("1234 Street 1", "", "City1", "State1", "12345"), "1234567890"),
                new Store("store_4567", new Address("1234 Street 2", "", "City2", "State2", "541321"), "0987654321")
        );
    }

    static void publishStores(List<Store> stores) {
        stores.forEach(store -> {
            try {
                var json = OBJECT_MAPPER.writeValueAsString(store);
                var metadata = publishMessageSync(OrdersTopology.STORES, store.locationId(), json);
                log.info("Published store: {}", metadata);
            } catch (Exception e) {
                log.error("Failed to publish store: {}", e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }
}
