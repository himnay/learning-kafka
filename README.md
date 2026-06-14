# Learning Kafka

A multi-module Maven project demonstrating Apache Kafka with Spring Boot 3.5, Java 25, Confluent Schema Registry, and Kafka Streams.

---

## Project Structure

```
learning-kafka/                       ← root POM (spring-boot-starter-parent)
├── kafka-core/                       ← Kafka producer + consumer
│   ├── library-events-producer/      ← REST API → Kafka (port 8080)
│   └── library-events-consumer/      ← Kafka → H2 database (port 8081)
├── kafka-stream/                     ← Kafka Streams
│   ├── orders-domain/                ← shared domain records (library JAR)
│   └── orders-streams-app/           ← order count & revenue topology (port 8082)
└── kafka-schema-registry/            ← Avro + Confluent Schema Registry
    ├── schemas/                      ← Avro .avsc definitions + code generation
    ├── coffee-orders-service/        ← Avro producer (port 8083)
    └── coffee-orders-consumer/       ← Avro consumer (port 8084)
```

---

## Tech Stack

| Concern | Technology |
|---|---|
| Language | Java 25 with virtual threads (Project Loom) |
| Framework | Spring Boot 3.5.0 |
| Messaging | Apache Kafka 3.9 — KRaft mode (no Zookeeper) |
| Schema | Confluent Schema Registry + Apache Avro 1.12 |
| Streams | Kafka Streams (via Spring Kafka) |
| Persistence | Spring Data JPA + H2 |
| Observability | Spring Actuator + Micrometer + Prometheus + Grafana |
| API Docs | springdoc-openapi (Swagger UI at `/swagger-ui.html`) |
| Testing | JUnit 5, Mockito, Awaitility, Spring Kafka Test (EmbeddedKafka), TestContainers |
| Build | Maven 3.9 (multi-module) |
| UI | Kafdrop (topic browser) |

---

## Gang of Four Design Patterns Applied

### Creational
| Pattern | Where |
|---|---|
| **Factory Method** | `@Bean` methods in `LibraryEventsConsumerConfig`, `AutoCreateConfig`, `OrdersStreamsConfiguration` — Spring's IoC container is the concrete factory |
| **Builder** | `Book`, `LibraryEvent` records use Lombok `@Builder`; `ProducerRecord` construction in `LibraryEventProducer.buildProducerRecord()` |

### Structural
| Pattern | Where |
|---|---|
| **Decorator** | `StreamsBuilderFactoryBeanConfigurer` wraps the default `StreamsBuilderFactoryBean` to add a custom uncaught exception handler |

### Behavioural
| Pattern | Where |
|---|---|
| **Strategy** | `ConsumerRecordRecoverer` in `LibraryEventsConsumerConfig.recoverer()` — swappable recovery (re-publish vs log-and-discard). `OrdersStreamsConfiguration` uses a DLQ recoverer vs log-skip. `OrderStoreService.countStoreName/revenueStoreName` selects the state store per order type. |
| **Template Method** | `LibraryEventsService.processLibraryEvent()` defines the algorithm skeleton; `save()` and `validate()` are the concrete steps dispatched by event type via a switch expression. `OrdersTopology.aggregateOrdersCountAndRevenue()` is the reusable skeleton for both GENERAL and RESTAURANT branches. |
| **Observer** | `LibraryEventsConsumer` annotated with `@KafkaListener` — Spring Kafka registers it as an observer that reacts to each Kafka record |

---

## Quick Start

### 1. Start the infrastructure

```bash
docker compose up -d
```

| Service | URL |
|---|---|
| Kafka (KRaft) | localhost:9092 |
| Schema Registry | http://localhost:8085 |
| Kafdrop (Kafka UI) | http://localhost:9000 |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3001 (admin/admin) |
| Swagger UI (producer) | http://localhost:8080/swagger-ui.html |

### 2. Build all modules

```bash
mvn clean install -DskipTests
```

### 3. Run the applications

```bash
# Terminal 1 — Producer
mvn spring-boot:run -pl kafka-core/library-events-producer

# Terminal 2 — Consumer
mvn spring-boot:run -pl kafka-core/library-events-consumer

# Terminal 3 — Streams
mvn spring-boot:run -pl kafka-stream/orders-streams-app
```

---

## API Reference — Library Events Producer (port 8080)

### POST /v1/libraryevent — Publish a new library event

```json
POST http://localhost:8080/v1/libraryevent
Content-Type: application/json

{
  "libraryEventId": null,
  "book": {
    "bookId": 456,
    "bookName": "Kafka Using Spring Boot",
    "bookAuthor": "Dilip"
  }
}
```

Response `201 Created`:
```json
{
  "libraryEventId": null,
  "libraryEventType": "NEW",
  "book": { "bookId": 456, "bookName": "Kafka Using Spring Boot", "bookAuthor": "Dilip" }
}
```

### PUT /v1/libraryevent — Update an existing library event

```json
PUT http://localhost:8080/v1/libraryevent
Content-Type: application/json

{
  "libraryEventId": 1,
  "book": {
    "bookId": 456,
    "bookName": "Kafka Using Spring Boot 3.x",
    "bookAuthor": "Dilip"
  }
}
```

Response `200 OK`.

---

## Observability

All modules expose Actuator endpoints:

| Endpoint | Description |
|---|---|
| `/actuator/health` | Liveness and readiness |
| `/actuator/prometheus` | Prometheus metrics scrape target |
| `/actuator/info` | Application metadata |

Import `insomnia-collection.json` into Insomnia to test all endpoints including actuator.

---

## Running Tests

```bash
# Unit + integration tests (all modules)
mvn test

# Only kafka-core tests
mvn test -pl kafka-core/library-events-producer,kafka-core/library-events-consumer
```

Integration tests use `@EmbeddedKafka` (no Docker needed). TestContainers is available as a dependency for adding container-backed tests.

---

## Module Details

### kafka-core/library-events-producer
- REST endpoints: `POST /v1/libraryevent`, `PUT /v1/libraryevent`
- Kafka producer with async `CompletableFuture`-based send
- Custom headers per record (`event-source: scanner`)
- `@ControllerAdvice` for validation error mapping

### kafka-core/library-events-consumer
- `@KafkaListener` consuming the `library-events` topic
- Persists `LibraryEvent` + `Book` entities to H2 via Spring Data JPA
- `DefaultErrorHandler` with `FixedBackOff` (3 attempts, 1 s delay)
- `IllegalArgumentException` is not retried; `RecoverableDataAccessException` triggers recovery (re-publish)

### kafka-stream/orders-streams-app
- `@EnableKafkaStreams` Spring Boot integration
- `OrdersTopology` builds the Kafka Streams topology: split orders by type, aggregate count and revenue
- Custom deserialization and serialization exception handlers
- Dead-letter topic (`orders-DLQ`) for unprocessable records

### kafka-schema-registry/schemas
- Avro `.avsc` schema definitions for `CoffeeOrder`, `OrderLineItem`, `Store`, etc.
- `avro-maven-plugin` generates Java classes into `target/generated-sources/avro` at build time

### kafka-schema-registry/coffee-orders-service
- Kafka producer using Confluent `KafkaAvroSerializer`
- Schemas registered automatically to `http://localhost:8085`

### kafka-schema-registry/coffee-orders-consumer
- Kafka consumer using Confluent `KafkaAvroDeserializer` with `specific.avro.reader=true`
