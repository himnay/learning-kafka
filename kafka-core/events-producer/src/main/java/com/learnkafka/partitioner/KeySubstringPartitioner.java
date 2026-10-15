package com.learnkafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * CUSTOM PARTITIONER for key-routing: every key that contains a configured substring goes to ONE
 * partition, so related records ("order-VIP-1", "VIP-member-7") stay together: one partition, read
 * by one consumer of a group, in order. Keys without a match are hashed like Kafka's default.
 *
 * Rules come from the producer property {@value #RULES_CONFIG}, comma-separated, checked in order
 * (first match wins; matching is case-sensitive):
 * <ul>
 *   <li>{@code VIP=0}: pinned, every key containing "VIP" goes to partition 0</li>
 *   <li>{@code UK}: hashed, murmur2("UK") % partitions. Kafka's key hash applied to the substring
 *       instead of the whole key: fixed, but you don't choose the number</li>
 * </ul>
 * Pinning doesn't reserve the partition: unmatched keys can still hash onto it
 * (the coffee orders' VipPartitionRule shows how to reserve one).
 *
 * Plugged in only for the key-routing template (KeyRoutingProducer).
 */
public class KeySubstringPartitioner implements Partitioner {

    /** Producer property carrying the rules, e.g. "VIP=0,UK,US". */
    public static final String RULES_CONFIG = "key.substring.partitions";

    /** A substring and the partition it is pinned to; {@code pinnedPartition} null = hash the substring. */
    public record Rule(String substring, Integer pinnedPartition) {

        int partitionFor(String topic, int numPartitions) {
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

    private List<Rule> rules = List.of();

    @Override
    public void configure(Map<String, ?> configs) {
        Object value = configs.get(RULES_CONFIG);
        rules = parseRules(value == null ? "" : value.toString());
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);
        if (key != null) {
            var rule = matchingRule(rules, key.toString());
            if (rule.isPresent()) {
                return rule.get().partitionFor(topic, numPartitions);
            }
        }
        if (keyBytes == null) {
            // A custom partitioner turns off Kafka's sticky partitioning for keyless records
            return ThreadLocalRandom.current().nextInt(numPartitions);
        }
        // Same as Kafka's built-in partitioner for keyed records
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
    }

    /** The first rule whose substring occurs in the key. */
    public static Optional<Rule> matchingRule(List<Rule> rules, String key) {
        return rules.stream().filter(rule -> key.contains(rule.substring())).findFirst();
    }

    /** Parses "VIP=0, UK, US" into rules; a malformed entry is a ConfigException. */
    public static List<Rule> parseRules(String rules) {
        var parsed = new ArrayList<Rule>();
        for (String raw : rules.split(",")) {
            var entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int eq = entry.lastIndexOf('=');
            var substring = (eq < 0 ? entry : entry.substring(0, eq)).trim();
            if (substring.isEmpty()) {
                throw new ConfigException(RULES_CONFIG, rules, "'" + entry + "' has no substring");
            }
            Integer partition = null;
            if (eq >= 0) {
                try {
                    partition = Integer.valueOf(entry.substring(eq + 1).trim());
                } catch (NumberFormatException e) {
                    throw new ConfigException(RULES_CONFIG, rules, "'" + entry + "' is not substring=partition");
                }
                if (partition < 0) {
                    throw new ConfigException(RULES_CONFIG, rules, "'" + entry + "' has a negative partition");
                }
            }
            parsed.add(new Rule(substring, partition));
        }
        return List.copyOf(parsed);
    }
}
