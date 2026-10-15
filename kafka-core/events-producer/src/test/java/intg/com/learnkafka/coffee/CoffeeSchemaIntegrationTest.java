package com.learnkafka.coffee;

import com.learnkafka.TestcontainersConfiguration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/** The producer owns a coffee schema: Flyway creates it in PostgreSQL at startup. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=mock://coffee-schema-test"
})
class CoffeeSchemaIntegrationTest {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Flyway creates coffee.coffee_order and coffee.outbox_event; the database runs with wal_level=logical")
    void flywayCreatesTheCoffeeSchema() {
        assertThat(jdbcTemplate.queryForList(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'coffee'", String.class))
                .contains("coffee_order", "outbox_event");
        assertThat(jdbcTemplate.queryForObject("SHOW wal_level", String.class)).isEqualTo("logical");
    }
}
