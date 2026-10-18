package com.learnkafka.partitioner;

import com.learnkafka.partitioner.model.Rule;
import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeySubstringPartitionerTest {

    private static final String TOPIC = "key-routing";
    private static final String RULES = "VIP=0, UK, US";

    private final KeySubstringPartitioner partitioner = new KeySubstringPartitioner();
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        partitioner.configure(Map.of(KeySubstringPartitioner.RULES_CONFIG, RULES));
        var node = new Node(1, "localhost", 9092);
        var partitions = IntStream.range(0, 3)
                .mapToObj(p -> new PartitionInfo(TOPIC, p, node, new Node[]{node}, new Node[]{node}))
                .toList();
        cluster = new Cluster("test", List.of(node), partitions, Set.of(), Set.of());
    }

    @Test
    @DisplayName("Every key containing a pinned substring goes to its partition, wherever the substring sits")
    void pinnedSubstring() {
        assertThat(List.of("VIP", "VIP-1", "order-VIP-7", "gold-VIP", "xVIPx"))
                .extracting(this::partitionFor)
                .containsOnly(0);
    }

    @Test
    @DisplayName("Keys containing a hashed substring share one partition: Kafka's murmur2 of the substring")
    void hashedSubstring() {
        int uk = BuiltInPartitioner.partitionForKey(bytes("UK"), 3);
        int us = BuiltInPartitioner.partitionForKey(bytes("US"), 3);

        assertThat(IntStream.range(0, 50).mapToObj(i -> "branch-UK-" + i).map(this::partitionFor)).containsOnly(uk);
        assertThat(IntStream.range(0, 50).mapToObj(i -> i + "-US").map(this::partitionFor)).containsOnly(us);
        assertThat(List.of(uk, us)).containsExactly(1, 2);   // what the README and Insomnia examples show
    }

    @Test
    @DisplayName("Rules are checked in configured order: the first matching substring wins")
    void firstMatchWins() {
        assertThat(partitionFor("VIP-US-9")).isZero();
        assertThat(partitionFor("US-VIP-9")).isZero();   // order of the rules, not of the key
    }

    @Test
    @DisplayName("Keys without a substring are hashed exactly like Kafka's built-in partitioner")
    void unmatchedKeysUseKafkaDefault() {
        for (int i = 0; i < 100; i++) {
            var key = "order-" + i;
            assertThat(partitionFor(key)).isEqualTo(BuiltInPartitioner.partitionForKey(bytes(key), 3));
        }
    }

    @Test
    @DisplayName("Matching is case-sensitive: vip is not VIP")
    void caseSensitive() {
        var rules = KeySubstringPartitioner.parseRules(RULES);

        assertThat(KeySubstringPartitioner.matchingRule(rules, "order-vip-1")).isEmpty();
        assertThat(partitionFor("order-vip-1"))
                .isEqualTo(BuiltInPartitioner.partitionForKey(bytes("order-vip-1"), 3));
    }

    @Test
    @DisplayName("A record without a key still gets a valid partition")
    void nullKey() {
        assertThat(IntStream.range(0, 100).map(i -> partitioner.partition(TOPIC, null, null, "v", null, cluster)))
                .allMatch(p -> p >= 0 && p < 3);
    }

    @Test
    @DisplayName("A pinned partition the topic doesn't have fails the send with a clear message")
    void pinnedPartitionOutOfRange() {
        partitioner.configure(Map.of(KeySubstringPartitioner.RULES_CONFIG, "VIP=5"));

        assertThatThrownBy(() -> partitionFor("VIP-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Rule VIP=5 needs partition 5, but topic key-routing has 3 partitions");
    }

    @Test
    @DisplayName("Rules parse into a substring and an optional pinned partition; blank entries are ignored")
    void parsesRules() {
        assertThat(KeySubstringPartitioner.parseRules(" VIP=0 , UK,, US "))
                .containsExactly(new Rule("VIP", 0), new Rule("UK", null), new Rule("US", null));
        assertThat(KeySubstringPartitioner.parseRules("")).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"VIP=x", "=1", "VIP=-1", "VIP=0,=2"})
    @DisplayName("Malformed rules are rejected with a ConfigException")
    void malformedRules(String rules) {
        assertThatThrownBy(() -> KeySubstringPartitioner.parseRules(rules))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(KeySubstringPartitioner.RULES_CONFIG);
    }

    private int partitionFor(String key) {
        return partitioner.partition(TOPIC, key, bytes(key), "value", null, cluster);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
