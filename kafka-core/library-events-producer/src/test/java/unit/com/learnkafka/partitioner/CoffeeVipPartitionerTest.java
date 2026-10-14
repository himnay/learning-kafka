package com.learnkafka.partitioner;

import com.learnavro.domain.generated.Address;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnavro.domain.generated.OrderLineItem;
import com.learnavro.domain.generated.PickUp;
import com.learnavro.domain.generated.Size;
import com.learnavro.domain.generated.Store;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class CoffeeVipPartitionerTest {

    private static final String TOPIC = "coffee-orders";

    private final CoffeeVipPartitioner partitioner = new CoffeeVipPartitioner();
    private Cluster cluster;

    @BeforeEach
    void setUp() {
        partitioner.configure(Map.of(CoffeeVipPartitioner.VIP_CUSTOMERS_CONFIG, "Ada, Grace"));
        var node = new Node(1, "localhost", 9092);
        var partitions = IntStream.range(0, 3)
                .mapToObj(p -> new PartitionInfo(TOPIC, p, node, new Node[]{node}, new Node[]{node}))
                .toList();
        cluster = new Cluster("test", List.of(node), partitions, Set.of(), Set.of());
    }

    @Test
    @DisplayName("VIP customers always go to partition 0")
    void vipGoesToPartitionZero() {
        for (int i = 0; i < 20; i++) {
            assertThat(partitionFor("Ada")).isZero();
        }
        assertThat(partitionFor("Grace")).isZero();
    }

    @Test
    @DisplayName("Everyone else is hashed over partitions 1..N-1, never 0, and the same key always maps the same way")
    void othersAvoidVipPartition() {
        var seen = IntStream.range(0, 200).map(i -> partitionFor("Linus")).boxed().toList();
        assertThat(seen).doesNotContain(0).contains(1, 2);

        var key = "fixed-key".getBytes(StandardCharsets.UTF_8);
        var order = order("Linus");
        assertThat(partitioner.partition(TOPIC, "fixed-key", key, order, null, cluster))
                .isEqualTo(partitioner.partition(TOPIC, "fixed-key", key, order, null, cluster));
    }

    private int partitionFor(String name) {
        var key = UUID.randomUUID().toString();
        return partitioner.partition(TOPIC, key, key.getBytes(StandardCharsets.UTF_8), order(name), null, cluster);
    }

    private static CoffeeOrder order(String name) {
        var store = Store.newBuilder().setId(1).setAddress(Address.newBuilder().setAddressLine1("1 Lane")
                .setCity("Seattle").setStateProvince("WA").setCountry("USA").setZip("98101").build()).build();
        var item = OrderLineItem.newBuilder().setName("Latte").setSize(Size.MEDIUM).setQuantity(1)
                .setCost(new BigDecimal("4.50")).build();
        return CoffeeOrder.newBuilder().setId(UUID.randomUUID()).setName(name).setNickName("").setStore(store)
                .setOrderLineItems(List.of(item)).setOrderedTime(Instant.now()).setPickUp(PickUp.IN_STORE)
                .setStatus("NEW").build();
    }
}
