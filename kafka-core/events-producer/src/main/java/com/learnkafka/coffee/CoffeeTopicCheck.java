package com.learnkafka.coffee;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.KafkaException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * At startup, compares app.coffee.partitions (what the VIP rule hashes over) with the real coffee-orders
 * topic. Debezium writes each event to the partition stored in the outbox row, so a topic with FEWER
 * partitions than configured would make it write to partitions that don't exist: refuse to start. More
 * partitions only leaves some unused (warning). If the topic can't be checked (missing, Kafka down), the
 * app starts anyway — writing an order must not depend on Kafka.
 */
@Slf4j
@Component
public class CoffeeTopicCheck implements ApplicationRunner {

    enum Outcome { OK, WARN_MORE_PARTITIONS, FAIL_FEWER_PARTITIONS, WARN_UNKNOWN }

    private static final long DESCRIBE_TIMEOUT_SECONDS = 10;

    private final KafkaAdmin kafkaAdmin;
    private final int configured;

    public CoffeeTopicCheck(KafkaAdmin kafkaAdmin, @Value("${app.coffee.partitions}") int configured) {
        this.kafkaAdmin = kafkaAdmin;
        this.configured = configured;
    }

    static Outcome decide(int configured, OptionalInt actual) {
        if (actual.isEmpty()) {
            return Outcome.WARN_UNKNOWN;
        }
        int partitions = actual.getAsInt();
        if (partitions < configured) {
            return Outcome.FAIL_FEWER_PARTITIONS;
        }
        return partitions > configured ? Outcome.WARN_MORE_PARTITIONS : Outcome.OK;
    }

    @Override
    public void run(ApplicationArguments args) {
        OptionalInt actual = describe();
        switch (decide(configured, actual)) {
            case OK -> log.info("Topic {} has {} partitions, as app.coffee.partitions says", CoffeeOrderService.TOPIC, configured);
            case WARN_MORE_PARTITIONS -> log.warn("Topic {} has {} partitions but app.coffee.partitions is {}: partitions {}..{} get no coffee orders",
                    CoffeeOrderService.TOPIC, actual.getAsInt(), configured, configured, actual.getAsInt() - 1);
            case FAIL_FEWER_PARTITIONS -> throw new IllegalStateException(
                    "Topic %s has %d partitions but app.coffee.partitions is %d: Debezium would be told to write to partitions that don't exist"
                            .formatted(CoffeeOrderService.TOPIC, actual.getAsInt(), configured));
            case WARN_UNKNOWN -> log.warn("Could not read the partition count of {} (topic missing or Kafka unreachable); assuming app.coffee.partitions={}",
                    CoffeeOrderService.TOPIC, configured);
        }
    }

    private OptionalInt describe() {
        AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        try {
            var description = admin.describeTopics(List.of(CoffeeOrderService.TOPIC)).allTopicNames()
                    .get(DESCRIBE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(CoffeeOrderService.TOPIC);
            return OptionalInt.of(description.partitions().size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return OptionalInt.empty();
        } catch (ExecutionException | TimeoutException | KafkaException e) {
            return OptionalInt.empty();
        } finally {
            admin.close(Duration.ofSeconds(2));
        }
    }
}
