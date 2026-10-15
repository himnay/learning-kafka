package com.learnkafka.coffee;

import org.apache.kafka.common.utils.Utils;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Which partition a coffee order belongs on: VIP customers on partition 0, everyone else hashed over
 * partitions 1..N-1, so partition 0 stays reserved for VIPs.
 *
 * This used to be a Kafka {@code Partitioner} (CoffeeVipPartitioner). With the outbox the app no longer
 * sends coffee orders itself: it writes the result into {@code outbox_event.partition_no}, and
 * Debezium's Outbox Event Router sends the record to that partition. Same arithmetic as before, so an
 * order id maps to the same partition it always did.
 */
public final class VipPartitionRule {

    public static final int VIP_PARTITION = 0;

    private final Set<String> vipCustomers;

    public VipPartitionRule(Set<String> vipCustomers) {
        this.vipCustomers = Set.copyOf(vipCustomers);
    }

    /** {@code "Ada, Grace"} → {Ada, Grace}; blank entries are dropped. */
    public static VipPartitionRule parse(String commaSeparated) {
        String[] names = commaSeparated == null ? new String[0] : commaSeparated.split(",");
        return new VipPartitionRule(Arrays.stream(names)
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toUnmodifiableSet()));
    }

    /**
     * @param customerName the order's customer (VIPs are matched exactly, case-sensitive)
     * @param key          the record key (the order id)
     * @param partitions   the topic's partition count
     */
    public int partition(String customerName, String key, int partitions) {
        if (partitions < 2 || vipCustomers.contains(customerName)) {
            return VIP_PARTITION;
        }
        return 1 + Utils.toPositive(Utils.murmur2(key.getBytes(UTF_8))) % (partitions - 1);
    }
}
