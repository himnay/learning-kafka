# Coffee orders outbox with Debezium — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /v1/coffee-orders` stores the order and its event in one PostgreSQL transaction; Debezium's Outbox Event Router publishes the event to `coffee-orders` with the same Avro bytes, key and VIP partition as today.

**Architecture:** The producer gets a `coffee` schema (Flyway) with `coffee_order` and `outbox_event`. `CoffeeOrderService` inserts the order, serializes the Avro `CoffeeOrder` with `KafkaAvroSerializer`, and inserts + deletes the outbox row in the same transaction. A Debezium PostgreSQL connector (3.7.0.Final, installed into the existing Kafka Connect worker) reads the WAL and routes each outbox INSERT to `coffee-orders` via `EventRouter` and `BinaryDataConverter`.

**Tech Stack:** Java 27, Spring Boot 4.1.1 (`JdbcClient`, Flyway), PostgreSQL 19 beta (compose) / 18 (Testcontainers), Confluent `KafkaAvroSerializer`, Kafka Connect cp 8.3.2, Debezium 3.7.0.Final, Testcontainers, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-15-coffee-orders-outbox-debezium-design.md`

## Global Constraints

- Producer-owned schema `coffee` in database `library`; Flyway history inside that schema (`spring.flyway.schemas: coffee`, `default-schema: coffee`).
- Table and column names exactly as in spec §3.2 (`id`, `aggregatetype`, `aggregateid`, `type`, `payload`, `partition_no`, `correlation_id`, `created_at`).
- `aggregatetype` = `coffee-orders`, `type` = `CoffeeOrder.PLACED`, `aggregateid` = order id string.
- Outbox row inserted and deleted in the same transaction as the `coffee_order` insert.
- `app.coffee.partitions: 3`; `app.coffee.vip-customers: Ada,Grace` (existing).
- Debezium plugin: `https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.7.0.Final/debezium-connector-postgres-3.7.0.Final-plugin.tar.gz`.
- Connector name `coffee-outbox`; slot `coffee_outbox_slot`; publication `coffee_outbox_publication`, `publication.autocreate.mode=all_tables` (the producer creates `coffee.outbox_event` only when it first starts, after the connector is registered).
- Consumers are not changed.
- Build/test from `kafka-core`: `mvn -B -pl events-producer -am test` (Docker must be running).
- Commit messages: conventional, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- The outbox write fails after the order insert: no `coffee_order` row may survive, and the client gets a 5xx — test in Task 3.
- `app.coffee.partitions` larger than the real topic: the app must refuse to start (Debezium would be told to write to a missing partition) — unit test of the check in Task 3.
- Kafka unreachable at startup: the app still starts (writing an order must not need Kafka) — unit test of the check in Task 3.
- Missing or blank `nickName`: stored as an empty string, as the Avro record requires — asserted in the integration test of Task 3.
- `X-Correlation-Id` absent: `correlation_id` is NULL and the header placement must not break the connector — asserted in Task 3 (column NULL) and in the end-to-end run in Task 4.

---

### Task 1: `VipPartitionRule`

**Files:**
- Create: `kafka-core/events-producer/src/main/java/com/learnkafka/coffee/VipPartitionRule.java`
- Test: `kafka-core/events-producer/src/test/java/unit/com/learnkafka/coffee/VipPartitionRuleTest.java`

**Interfaces:**
- Produces: `public final class VipPartitionRule` with `public static final int VIP_PARTITION = 0;`, constructor `public VipPartitionRule(Set<String> vipCustomers)`, factory `public static VipPartitionRule parse(String commaSeparated)` (trims, drops blanks), and `public int partition(String customerName, String key, int partitions)` — `0` if `partitions < 2` or the customer is a VIP, else `1 + Utils.toPositive(Utils.murmur2(key.getBytes(UTF_8))) % (partitions - 1)` (the current `CoffeeVipPartitioner` arithmetic, so existing keys keep their partitions).

- [ ] **Step 1: Write the failing tests** (ported from `CoffeeVipPartitionerTest`, rule built with `VipPartitionRule.parse("Ada, Grace")`, 3 partitions):

```java
@Test void vipGoesToPartitionZero()                 // "Ada" ×20 and "Grace" → 0
@Test void othersAvoidVipPartition()                // "Linus" with 200 random UUID keys → never 0, contains 1 and 2
@Test void sameKeyAlwaysSamePartition()             // "Linus","fixed-key" twice → equal
@Test void singlePartitionTopic_alwaysZero()         // partitions 1 → 0 for "Linus"
@Test void matchesTheOldPartitionerForTheSameKey()  // "fixed-key" → 1 + toPositive(murmur2("fixed-key" UTF-8)) % 2 computed by hand: write the literal value in the test
```

- [ ] **Step 2: Run to verify they fail** — `mvn -B -pl events-producer -am test -Dtest=VipPartitionRuleTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: compilation failure.
- [ ] **Step 3: Implement `VipPartitionRule`.**
- [ ] **Step 4: Run to verify they pass** — expected `Tests run: 5, Failures: 0`.
- [ ] **Step 5: Commit** — `feat(kafka-core): VIP partition rule as a plain function`

### Task 2: Producer database plumbing

**Files:**
- Modify: `kafka-core/events-producer/pom.xml` — add `spring-boot-starter-jdbc`, `postgresql` (runtime), `spring-boot-flyway`, `flyway-core`, `flyway-database-postgresql`; test: `testcontainers-postgresql`, `spring-boot-testcontainers` (same artifacts the consumer pom uses, versions from the parent BOM)
- Modify: `kafka-core/events-producer/src/main/resources/application.yml` — `spring.datasource` (same URL/user/password placeholders as the consumers: `jdbc:postgresql://${POSTGRES_HOST:localhost}:${POSTGRES_PORT:5435}/${POSTGRES_DB:library}`), `spring.flyway.schemas: coffee`, `spring.flyway.default-schema: coffee`, `app.coffee.partitions: 3`
- Create: `kafka-core/events-producer/src/main/resources/db/migration/V1__create_coffee_order_and_outbox_event.sql` — the two tables of spec §3.2, with its column comments
- Create: `kafka-core/events-producer/src/test/java/intg/com/learnkafka/TestcontainersConfiguration.java` — like the consumer's, `new PostgreSQLContainer("postgres:18").withCommand("postgres", "-c", "wal_level=logical")`
- Modify: the four `@SpringBootTest` classes in `src/test/java/intg/...` (`LibraryEventsControllerIntegrationTest`, `CompressionIntegrationTest`, `KeyRoutingIntegrationTest`, `ExplicitPartitionIntegrationTest`) — add `@Import(TestcontainersConfiguration.class)`
- Test: `kafka-core/events-producer/src/test/java/intg/com/learnkafka/coffee/CoffeeSchemaIntegrationTest.java`

**Interfaces:**
- Produces: tables `coffee.coffee_order`, `coffee.outbox_event`; `TestcontainersConfiguration` for every producer Spring Boot test.

- [ ] **Step 1: Write the failing test** — `@SpringBootTest @Import(TestcontainersConfiguration.class)` with the embedded-Kafka properties the other producer tests use:

```java
@Test void flywayCreatesTheCoffeeSchema()
    // jdbcTemplate.queryForList("SELECT table_name FROM information_schema.tables WHERE table_schema = 'coffee'", String.class)
    //   contains "coffee_order", "outbox_event"
    // jdbcTemplate.queryForObject("SHOW wal_level", String.class) == "logical"
```

- [ ] **Step 2: Run to verify it fails** — `-Dtest=CoffeeSchemaIntegrationTest`. Expected: FAIL (no datasource / no tables).
- [ ] **Step 3: Add dependencies, configuration, migration, Testcontainers configuration, and the `@Import` on the four existing tests.**
- [ ] **Step 4: Run the module's whole suite** — `mvn -B -pl events-producer -am test`. Expected: BUILD SUCCESS (the 36 existing tests + the new one).
- [ ] **Step 5: Commit** — `build(kafka-core): producer gets its own coffee schema (Flyway, PostgreSQL)`

### Task 3: Outbox write path replaces the coffee `KafkaTemplate`

**Files:**
- Create in `kafka-core/events-producer/src/main/java/com/learnkafka/coffee/`: `CoffeeOrdersController.java`, `CoffeeOrderService.java`, `CoffeeOrderRepository.java`, `OutboxRepository.java`, `OutboxEvent.java`, `CoffeeOrderPayloadSerializer.java`, `CoffeeTopicCheck.java`
- Delete: `coffee/CoffeeOrdersProducer.java`, `partitioner/CoffeeVipPartitioner.java`, `test/.../coffee/CoffeeOrdersProducerTest.java`, `test/.../partitioner/CoffeeVipPartitionerTest.java`
- Test: `kafka-core/events-producer/src/test/java/intg/com/learnkafka/coffee/CoffeeOutboxIntegrationTest.java`, `kafka-core/events-producer/src/test/java/unit/com/learnkafka/coffee/CoffeeOrdersControllerTest.java`, `kafka-core/events-producer/src/test/java/unit/com/learnkafka/coffee/CoffeeTopicCheckTest.java`

**Interfaces:**
- Consumes: `VipPartitionRule` (Task 1); tables (Task 2); existing `EventMetadataFactory.newEvent(String, String)`, `CoffeeOrderEvent.placed(...)`, `CoffeeOrderResponse.from(CoffeeOrder, EventMetadata)`.
- Produces:
  - `public record OutboxEvent(UUID id, String aggregateType, String aggregateId, String type, byte[] payload, int partition, @Nullable String correlationId)`
  - `OutboxRepository.append(OutboxEvent event)` — INSERT then DELETE by id (`JdbcClient`), joins the caller's transaction
  - `CoffeeOrderRepository.insert(CoffeeOrder order)`
  - `CoffeeOrderPayloadSerializer.serialize(CoffeeOrder order) -> byte[]` — a `KafkaAvroSerializer` configured with `spring.kafka.producer.properties.schema.registry.url`, `serialize("coffee-orders", order)`; closed on shutdown
  - `CoffeeOrderService.place(CoffeeOrderRequest request, @Nullable String correlationId) -> CoffeeOrderResponse` — `@Transactional`; builds the `CoffeeOrder` exactly as `CoffeeOrdersProducer.buildCoffeeOrder` does today (move that method here)
  - `CoffeeOrdersController` — `POST /v1/coffee-orders`, `@ResponseStatus(CREATED)`, same `X-Correlation-Id` header, delegates to the service
  - `CoffeeTopicCheck` — `ApplicationRunner`; `static Outcome decide(int configured, OptionalInt actual)` with `enum Outcome { OK, WARN_MORE_PARTITIONS, FAIL_FEWER_PARTITIONS, WARN_UNKNOWN }`; `run` describes `coffee-orders` with a 10 s timeout, throws `IllegalStateException` on `FAIL_FEWER_PARTITIONS`, logs a warning for the two warnings

- [ ] **Step 1: Write the failing tests**

`CoffeeOutboxIntegrationTest` (`@SpringBootTest(webEnvironment = RANDOM_PORT)`, `@Import(TestcontainersConfiguration.class)`, embedded Kafka + mock registry `mock://coffee-outbox-test`). Before each test `SELECT pg_create_logical_replication_slot('outbox_test', 'test_decoding')`; after each `SELECT pg_drop_replication_slot('outbox_test')`. Helper `List<String> changes()` = `SELECT data FROM pg_logical_slot_get_changes('outbox_test', NULL, NULL)`.

```java
@Test void vipOrder_isStoredWithItsEventInOneTransaction()
    // POST {"name":"Ada","nickName":"a"} with X-Correlation-Id "corr-1" → 201, read id from the response
    // changes(): BEGIN, INSERT coffee.coffee_order (id), INSERT coffee.outbox_event, DELETE coffee.outbox_event, COMMIT — in that order, one BEGIN/COMMIT pair
    // outbox INSERT line: aggregatetype 'coffee-orders', aggregateid '<id>', type 'CoffeeOrder.PLACED', partition_no 0, correlation_id 'corr-1'
    // payload[bytea] hex → KafkaAvroDeserializer(mock://coffee-outbox-test, specific reader) → CoffeeOrder with that id and name "Ada"
    // SELECT count(*) FROM coffee.outbox_event == 0
@Test void otherCustomer_getsANonVipPartition_andNullCorrelationId()
    // POST {"name":"Linus"} without the header → outbox INSERT has partition_no 1 or 2, correlation_id null; coffee_order.nick_name == ''
@Test void failingOutboxWrite_rollsTheOrderBack()
    // @MockitoSpyBean OutboxRepository; doThrow(new DataAccessResourceFailureException("outbox down")).when(spy).append(any())
    // POST {"name":"Rollback"} → 5xx; SELECT count(*) FROM coffee.coffee_order WHERE name = 'Rollback' == 0
```

`CoffeeOrdersControllerTest` (standalone MockMvc, mocked `CoffeeOrderService`):

```java
@Test void returns201WithTheServiceResponse()   // jsonPath $.name "Grace", $.metadata.eventType "CoffeeOrder.PLACED"
@Test void blankNameRejected()                  // {"name":""} → 400, service never called
```

`CoffeeTopicCheckTest`:

```java
@Test void sameCount_ok()                 // decide(3, OptionalInt.of(3)) == OK
@Test void topicHasMore_warns()           // decide(3, OptionalInt.of(6)) == WARN_MORE_PARTITIONS
@Test void topicHasFewer_fails()          // decide(3, OptionalInt.of(2)) == FAIL_FEWER_PARTITIONS
@Test void topicUnknown_warns()           // decide(3, OptionalInt.empty()) == WARN_UNKNOWN
```

- [ ] **Step 2: Run to verify they fail** — `mvn -B -pl events-producer -am test -Dtest='CoffeeOutboxIntegrationTest,CoffeeOrdersControllerTest,CoffeeTopicCheckTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: compilation failure.
- [ ] **Step 3: Implement the classes listed in Interfaces; delete the four files listed above** (the coffee `KafkaTemplate` goes with `CoffeeOrdersProducer`). Parse `test_decoding` output with a regex per column (`payload\[bytea\]:'\\x([0-9a-f]+)'`, `partition_no\[integer\]:(\d+)`, …) and `HexFormat.of().parseHex`.
- [ ] **Step 4: Run the module's whole suite** — `mvn -B -pl events-producer -am test`. Expected: BUILD SUCCESS.
- [ ] **Step 5: Commit** — `feat(kafka-core): coffee orders go through a transactional outbox`

### Task 4: Debezium in docker compose, end-to-end check, docs

**Files:**
- Create: `kafka-core/connect/coffee-outbox-debezium.json` — spec §3.4's JSON with `publication.autocreate.mode` set to `all_tables`
- Modify: `kafka-core/docker-compose.yml`:
  - `postgres`: `command: postgres -c wal_level=logical`
  - new service `debezium-plugin` (`curlimages/curl:8.11.1`, `user: root`, `restart: "no"`, volume `kafka-core-connect-plugins:/plugins`): if `/plugins/debezium-connector-postgres` is missing, download the plugin URL from Global Constraints and untar it into `/plugins`
  - `kafka-connect`: `depends_on: debezium-plugin: { condition: service_completed_successfully }`, volume `kafka-core-connect-plugins:/usr/share/debezium-plugins:ro`, `CONNECT_PLUGIN_PATH` += `,/usr/share/debezium-plugins`, healthcheck requires both `JdbcSinkConnector` and `PostgresConnector` in `GET /connector-plugins`
  - `kafka-connect-init`: `entrypoint: ["sh", "-c"]`, PUT both connector configs (`library-events-jdbc-sink`, `coffee-outbox`)
  - `kafka-init`: create `coffee-orders` (3 partitions, RF 3, `min.insync.replicas=2`)
  - `volumes`: `kafka-core-connect-plugins:`
- Modify: `kafka-core/README.md` — new last section "32. Transactional outbox for coffee orders (Debezium)" + TOC line (flow diagram from spec §3.1, tables, connector JSON with each option explained, insert-and-delete, `all_tables` and why, heartbeats and WAL, `pg_replication_slots` inspection, slot clean-up on teardown, at-least-once, Schema Registry still needed when an order is written); §9 coffee section; §22 (second connector); §25 (VIP rule now via `partition_no`; `CoffeeVipPartitioner` gone); §18; §19; §31 test counts
- Modify: `README.md` (root) — coffee-orders API note, module details, test counts; `insomnia-collection.json` coffee request descriptions mention the outbox

- [ ] **Step 1: Bring the stack up** — `docker compose up -d` in `kafka-core`. Expected: `debezium-plugin` exits 0; `curl -s localhost:8083/connector-plugins | grep -c PostgresConnector` → 1; `curl -s localhost:8083/connectors/coffee-outbox/status` → connector and task `RUNNING`; `docker exec kafka-core-postgres psql -U library -c 'SHOW wal_level'` → `logical`.
- [ ] **Step 2: Start the producer and a consumer; POST `Ada` (with `X-Correlation-Id: e2e-1`) and `Linus`.** Expected: `kafka-avro-console-consumer --topic coffee-orders --property print.headers=true --property print.partition=true` (inside the schema-registry container) shows both orders, Ada on partition 0, headers `id`, `event-type:CoffeeOrder.PLACED`, `correlation-id:e2e-1` (Linus: no correlation header or empty); the consumer logs both.
- [ ] **Step 3: Outage check** — `docker stop kafka-core-kafka-connect`; POST `Grace` → 201; `docker start kafka-core-kafka-connect`; within a minute `Grace` is on `coffee-orders`.
- [ ] **Step 4: Write the docs listed above, using the real output from Steps 1–3.**
- [ ] **Step 5: Run all kafka-core tests** — `cd kafka-core && mvn -B test`. Expected: BUILD SUCCESS.
- [ ] **Step 6: Commit and push** — `feat(kafka-core): Debezium outbox connector for coffee orders; docs`, then `git push origin main`.
