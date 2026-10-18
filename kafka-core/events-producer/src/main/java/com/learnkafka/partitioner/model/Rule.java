package com.learnkafka.partitioner.model;

import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;

/** A substring and the partition it is pinned to; {@code pinnedPartition} null = hash the substring. */
public record Rule(String substring, Integer pinnedPartition) {

    public int partitionFor(String topic, int numPartitions) {
        if (pinnedPartition == null) {
            return Utils.toPositive(Utils.murmur2(substring.getBytes(StandardCharsets.UTF_8))) % numPartitions;
        }
        if (pinnedPartition >= numPartitions) {
            // Kafka does not check this: the record would wait for a partition that never appears
            throw new IllegalArgumentException("Rule " + substring + "=" + pinnedPartition + " needs partition "
                    + pinnedPartition + ", but topic " + topic + " has " + numPartitions + " partitions");
        }
        return pinnedPartition;
    }
}
