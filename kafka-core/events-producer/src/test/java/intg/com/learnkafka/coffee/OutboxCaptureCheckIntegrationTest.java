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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** OutboxCaptureCheck against a real PostgreSQL (wal_level=logical, no Debezium slot by default). */
@SpringBootTest
@EmbeddedKafka(partitions = 3)
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=mock://coffee-schema-test"
})
class OutboxCaptureCheckIntegrationTest {

    @Autowired
    JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("Required and the slot is missing: refuse to start, naming the slot and the fix")
    void missingSlot_refusesToStart() {
        assertThatThrownBy(() -> new OutboxCaptureCheck(jdbcTemplate, "it_missing_slot", true).afterPropertiesSet())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("it_missing_slot")
                .hasMessageContaining("docker compose up -d");
    }

    @Test
    @DisplayName("The slot exists: start normally")
    void existingSlot_passes() {
        jdbcTemplate.queryForList("SELECT * FROM pg_create_logical_replication_slot('it_existing_slot', 'pgoutput')");
        try {
            assertThatCode(() -> new OutboxCaptureCheck(jdbcTemplate, "it_existing_slot", true).afterPropertiesSet())
                    .doesNotThrowAnyException();
        } finally {
            jdbcTemplate.queryForList("SELECT pg_drop_replication_slot('it_existing_slot')");
        }
    }

    @Test
    @DisplayName("Not required (tests, ad-hoc runs): only a warning")
    void notRequired_onlyWarns() {
        assertThatCode(() -> new OutboxCaptureCheck(jdbcTemplate, "it_missing_slot", false).afterPropertiesSet())
                .doesNotThrowAnyException();
    }
}
