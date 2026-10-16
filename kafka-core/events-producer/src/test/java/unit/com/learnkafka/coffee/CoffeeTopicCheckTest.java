package com.learnkafka.coffee;

import com.learnkafka.coffee.CoffeeTopicCheck.Outcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.Map;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@DisplayName("CoffeeTopicCheck: app.coffee.partitions against the real coffee-orders topic")
class CoffeeTopicCheckTest {

    @Test
    @DisplayName("Same partition count: fine")
    void sameCount_ok() {
        assertThat(CoffeeTopicCheck.decide(3, OptionalInt.of(3))).isEqualTo(Outcome.OK);
    }

    @Test
    @DisplayName("The topic has more partitions than configured: a warning (the extra ones get no orders)")
    void topicHasMore_warns() {
        assertThat(CoffeeTopicCheck.decide(3, OptionalInt.of(6))).isEqualTo(Outcome.WARN_MORE_PARTITIONS);
    }

    @Test
    @DisplayName("The topic has fewer partitions than configured: refuse to start")
    void topicHasFewer_fails() {
        assertThat(CoffeeTopicCheck.decide(3, OptionalInt.of(2))).isEqualTo(Outcome.FAIL_FEWER_PARTITIONS);
    }

    @Test
    @DisplayName("A Kafka bootstrap host that doesn't resolve is 'unreachable' too: the check warns, startup goes on")
    void unresolvableBootstrapHost_onlyWarns() {
        var admin = new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "no-such-host.invalid:9092"));

        assertThatCode(() -> new CoffeeTopicCheck(admin, 3).run(null)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Topic missing or Kafka unreachable: a warning, the app still starts")
    void topicUnknown_warns() {
        assertThat(CoffeeTopicCheck.decide(3, OptionalInt.empty())).isEqualTo(Outcome.WARN_UNKNOWN);
    }
}
