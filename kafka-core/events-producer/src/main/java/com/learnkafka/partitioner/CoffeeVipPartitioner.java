package com.learnkafka.partitioner;

import com.learnavro.domain.generated.CoffeeOrder;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CUSTOM PARTITIONER for coffee orders: VIP customers get partition 0 to themselves, everyone
 * else is hashed (murmur2, like Kafka's default) over partitions 1..N-1.
 *
 * Plugged in only for the coffee-orders template (CoffeeOrdersProducer), so library events keep
 * Kafka's built-in partitioner — including sticky partitioning for their null keys, which a
 * custom partitioner would turn off.
 */
public class CoffeeVipPartitioner implements Partitioner {

    /** Producer property carrying the VIP names (comma-separated). */
    public static final String VIP_CUSTOMERS_CONFIG = "coffee.vip.customers";

    public static final int VIP_PARTITION = 0;

    private Set<String> vipCustomers = Set.of();

    @Override
    public void configure(Map<String, ?> configs) {
        Object vips = configs.get(VIP_CUSTOMERS_CONFIG);
        if (vips != null) {
            vipCustomers = Arrays.stream(vips.toString().split(","))
                    .map(String::trim).filter(v -> !v.isEmpty())
                    .collect(Collectors.toUnmodifiableSet());
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (numPartitions < 2) {
            return 0;
        }
        if (value instanceof CoffeeOrder order && vipCustomers.contains(order.getName().toString())) {
            return VIP_PARTITION;
        }
        // Coffee orders always have a key (the order UUID)
        return 1 + Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - 1);
    }

    @Override
    public void close() {
    }
}
