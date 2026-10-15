package com.learnkafka.coffee;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("VipPartitionRule: VIP customers on partition 0, everyone else hashed over the rest")
class VipPartitionRuleTest {

    private static final int PARTITIONS = 3;

    private final VipPartitionRule rule = VipPartitionRule.parse("Ada, Grace");

    @Test
    @DisplayName("VIP customers always go to partition 0")
    void vipGoesToPartitionZero() {
        for (int i = 0; i < 20; i++) {
            assertThat(rule.partition("Ada", UUID.randomUUID().toString(), PARTITIONS)).isZero();
        }
        assertThat(rule.partition("Grace", UUID.randomUUID().toString(), PARTITIONS)).isZero();
    }

    @Test
    @DisplayName("Everyone else is hashed over partitions 1..N-1 and never lands on 0")
    void othersAvoidVipPartition() {
        var seen = IntStream.range(0, 200)
                .map(i -> rule.partition("Linus", UUID.randomUUID().toString(), PARTITIONS))
                .boxed().toList();

        assertThat(seen).doesNotContain(0).contains(1, 2);
    }

    @Test
    @DisplayName("The same key always maps to the same partition")
    void sameKeyAlwaysSamePartition() {
        assertThat(rule.partition("Linus", "fixed-key", PARTITIONS))
                .isEqualTo(rule.partition("Linus", "fixed-key", PARTITIONS));
    }

    @Test
    @DisplayName("A single-partition topic has nowhere else to go: always 0")
    void singlePartitionTopic_alwaysZero() {
        assertThat(rule.partition("Linus", "fixed-key", 1)).isZero();
    }

    @Test
    @DisplayName("Same arithmetic as the old CoffeeVipPartitioner: existing keys keep their partitions")
    void matchesTheOldPartitionerForTheSameKey() {
        // 1 + toPositive(murmur2("fixed-key")) % 2, computed outside Java: murmur2 = -79829869 → 2067653779 % 2 = 1
        assertThat(rule.partition("Linus", "fixed-key", PARTITIONS)).isEqualTo(2);
    }
}
