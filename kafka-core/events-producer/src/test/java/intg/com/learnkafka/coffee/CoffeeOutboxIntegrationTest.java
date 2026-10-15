package com.learnkafka.coffee;

import com.learnavro.domain.generated.CoffeeOrder;
import com.learnkafka.TestcontainersConfiguration;
import com.learnkafka.domain.CoffeeOrderRequest;
import com.learnkafka.domain.CoffeeOrderResponse;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestClient;

import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

/**
 * POST /v1/coffee-orders against a real PostgreSQL. A logical replication slot with the test_decoding
 * plugin shows exactly what Debezium will read from the write-ahead log.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
@EmbeddedKafka(partitions = 3)
@TestPropertySource(properties = {
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.schema.registry.url=" + CoffeeOutboxIntegrationTest.REGISTRY
})
class CoffeeOutboxIntegrationTest {

    static final String REGISTRY = "mock://coffee-outbox-test";
    private static final String SLOT = "outbox_test";
    private static final Pattern PAYLOAD = Pattern.compile("payload\\[bytea\\]:'\\\\x([0-9a-f]+)'");
    private static final Pattern PARTITION = Pattern.compile("partition_no\\[integer\\]:(\\d+)");

    @LocalServerPort
    int port;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @MockitoSpyBean
    OutboxRepository outboxSpy;

    @BeforeEach
    void createSlot() {
        jdbcTemplate.queryForList("SELECT * FROM pg_create_logical_replication_slot(?, 'test_decoding')", SLOT);
    }

    @AfterEach
    void dropSlot() {
        jdbcTemplate.queryForList("SELECT pg_drop_replication_slot(?)", SLOT);
    }

    @Test
    @DisplayName("A VIP order and its event commit together: order INSERT, outbox INSERT (partition 0), outbox DELETE")
    void vipOrder_isStoredWithItsEventInOneTransaction() {
        var response = post("Ada", "a", "corr-1");

        var changes = changes();
        assertThat(changes).hasSize(5);
        assertThat(changes.get(0)).startsWith("BEGIN");
        assertThat(changes.get(1)).startsWith("table coffee.coffee_order: INSERT:")
                .contains("id[uuid]:'" + response.id() + "'");
        var outboxInsert = changes.get(2);
        assertThat(outboxInsert).startsWith("table coffee.outbox_event: INSERT:")
                .contains("id[uuid]:'" + response.metadata().eventId() + "'")
                .contains("aggregatetype[character varying]:'coffee-orders'")
                .contains("aggregateid[character varying]:'" + response.id() + "'")
                .contains("type[character varying]:'CoffeeOrder.PLACED'")
                .contains("correlation_id[character varying]:'corr-1'");
        assertThat(partitionOf(outboxInsert)).isZero();
        assertThat(changes.get(3)).startsWith("table coffee.outbox_event: DELETE:");
        assertThat(changes.get(4)).startsWith("COMMIT");

        CoffeeOrder order = decode(payloadOf(outboxInsert));
        assertThat(order.getId().toString()).isEqualTo(response.id());
        assertThat(order.getName().toString()).isEqualTo("Ada");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM coffee.outbox_event", Integer.class)).isZero();
    }

    @Test
    @DisplayName("Anyone else gets partition 1 or 2; without X-Correlation-Id the column is NULL; no nick name is stored as ''")
    void otherCustomer_getsANonVipPartition_andNullCorrelationId() {
        var response = post("Linus", null, null);

        var outboxInsert = changes().stream()
                .filter(line -> line.startsWith("table coffee.outbox_event: INSERT:"))
                .findFirst().orElseThrow();
        assertThat(partitionOf(outboxInsert)).isIn(1, 2);
        assertThat(outboxInsert).contains("correlation_id[character varying]:null");
        assertThat(jdbcTemplate.queryForObject("SELECT nick_name FROM coffee.coffee_order WHERE id = ?",
                String.class, UUID.fromString(response.id()))).isEmpty();
    }

    @Test
    @DisplayName("If writing the event fails, the order is rolled back too and the client gets a 5xx")
    void failingOutboxWrite_rollsTheOrderBack() {
        doThrow(new DataAccessResourceFailureException("outbox down")).when(outboxSpy).append(any());

        assertThatThrownBy(() -> post("Rollback", null, null)).isInstanceOf(HttpServerErrorException.class);

        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM coffee.coffee_order WHERE name = 'Rollback'",
                Integer.class)).isZero();
    }

    private CoffeeOrderResponse post(String name, String nickName, String correlationId) {
        var request = RestClient.create("http://localhost:" + port).post().uri("/v1/coffee-orders")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new CoffeeOrderRequest(name, nickName));
        if (correlationId != null) {
            request = request.header("X-Correlation-Id", correlationId);
        }
        return request.retrieve().body(CoffeeOrderResponse.class);
    }

    /** Everything committed since the slot was created, one line per change (test_decoding format). */
    private List<String> changes() {
        return jdbcTemplate.queryForList("SELECT data FROM pg_logical_slot_get_changes(?, NULL, NULL)", String.class, SLOT);
    }

    private static byte[] payloadOf(String line) {
        var matcher = PAYLOAD.matcher(line);
        assertThat(matcher.find()).as("payload column in %s", line).isTrue();
        return HexFormat.of().parseHex(matcher.group(1));
    }

    private static int partitionOf(String line) {
        var matcher = PARTITION.matcher(line);
        assertThat(matcher.find()).as("partition_no column in %s", line).isTrue();
        return Integer.parseInt(matcher.group(1));
    }

    private static CoffeeOrder decode(byte[] confluentAvro) {
        try (var deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY,
                    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true), false);
            return (CoffeeOrder) deserializer.deserialize("coffee-orders", confluentAvro);
        }
    }
}
