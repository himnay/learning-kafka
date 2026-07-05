# kafka-core — Library Events Producer & Consumer

This module is the "plain Kafka" half of the repo — no Avro, no Streams, just a REST-facing producer, a JSON-over-Kafka event, and a consumer with retry/dead-letter handling built on Spring Kafka's `DefaultErrorHandler`. It is the best place to start if you want to see the fundamentals: producer records, consumer groups, offsets, and what "at-least-once delivery" actually looks like in application code.

For the broader repo architecture and core Kafka vocabulary, see the [root README](../README.md). This document goes deep on what's *specific* to this module.

---

## Modules

```
kafka-core/
├── library-events-producer/   REST API (port 8080) → publishes to "library-events"
└── library-events-consumer/   @KafkaListener (port 8081) → persists to H2
```

---

## The event: `LibraryEvent`

Both sides define their own copy of the shape (a `record` on the producer side annotated with Bean Validation, a JPA `@Entity` on the consumer side) — they are *not* a shared library dependency. This is a deliberate, common real-world pattern: producer and consumer evolve independently and only agree on the **wire format** (JSON), not a shared Java type. The cost is that a field rename on one side silently breaks the other at runtime instead of at compile time — one of the reasons the `kafka-schema-registry` module exists to show the Avro alternative, which enforces compatibility at publish time instead.

Producer-side shape (`library-events-producer/.../domain/LibraryEvent.java`):

```java
public record LibraryEvent(
        Integer libraryEventId,
        LibraryEventType libraryEventType,   // NEW | UPDATE
        @NotNull @Valid Book book
) {}

public record Book(
        @NotNull Integer bookId,
        @NotBlank String bookName,
        @NotBlank String bookAuthor
) {}
```

Wire format (what actually goes on the topic — a JSON string value, `Integer` key):

```json
{
  "libraryEventId": 456,
  "libraryEventType": "NEW",
  "book": { "bookId": 456, "bookName": "Kafka Using Spring Boot", "bookAuthor": "Dilip" }
}
```

The **key** is `libraryEventId` (an `Integer`, serialized with `IntegerSerializer`/`IntegerDeserializer`). Keying by event id means updates and news for the *same* library event id always land on the same partition, which preserves per-entity ordering — important because an `UPDATE` for a given book must never be processed before the `NEW` that created it, and per-partition ordering is Kafka's only ordering guarantee.

---

## Producer side: `LibraryEventProducer`

Three send methods, each demonstrating a different Spring Kafka `KafkaTemplate` usage pattern:

| Method | Used by | Behavior |
|---|---|---|
| `sendLibraryEventWithHeaders()` | `POST` / `PUT /v1/libraryevent` | Async send via an explicit `ProducerRecord` carrying a custom header `event-source: scanner`; returns a `CompletableFuture` the controller does not block on |
| `sendLibraryEvent()` | available, not wired to a controller endpoint | Async send using `kafkaTemplate.sendDefault()` (the configured default topic) — no custom headers |
| `sendLibraryEventSynchronous()` | available, not wired to a controller endpoint | Blocks up to 1 second via `.get(1, TimeUnit.SECONDS)` — shown as the pattern to reach for only when the caller genuinely needs a confirmed offset before responding |

`LibraryEventsController` calls `sendLibraryEventWithHeaders()` for both `POST` (sets `LibraryEventType.NEW`) and `PUT` (sets `LibraryEventType.UPDATE`, and 400s if `libraryEventId` is missing) and returns `201`/`200` to the HTTP caller **without waiting for the Kafka send to complete** — the send's success/failure is only observed by the `whenComplete()` callback (`handleSuccess` / `handleFailure`), which just logs. This is a fire-and-forget-from-the-HTTP-caller's-perspective design: the REST API's availability is decoupled from Kafka's availability, at the cost of the HTTP response not reflecting whether the message actually made it onto the topic.

### Producer reliability configuration (`application.yml`, `local` profile)

```yaml
spring.kafka.producer.properties:
  acks: all                 # wait for all in-sync replicas to ack
  retries: 10                # retry transient send failures
  retry.backoff.ms: 1000
  enable.idempotence: "true" # producer-side dedup of retried sends
```

- **`acks=all`** is the strongest per-record durability the producer can ask for: the leader only acks once every in-sync replica has the record, so a leader crash immediately after acking doesn't lose data.
- **`enable.idempotence=true`** turns on the idempotent producer, which assigns each producer instance a PID and sequence number per partition. If a `retries`-triggered resend duplicates a request the broker already wrote, the broker recognizes the duplicate sequence number and drops it instead of appending it twice. This eliminates *producer-retry* duplicates — it says nothing about the consumer redelivering a record it already fully processed, which is a separate, application-level concern (see the consumer section below).
- **Auto-topic-creation** (`AutoCreateConfig`, `@Profile("local")`) declares `library-events` with `3` partitions / `1` replica via `NewTopic` — only active in the `local` profile so production topic configuration (partition count, replication factor) is managed deliberately outside the app.

---

## Consumer side: `LibraryEventsConsumer` → `LibraryEventsService`

```java
@KafkaListener(topics = {"library-events"}, groupId = "${spring.kafka.consumer.group-id}")
public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
    libraryEventsService.processLibraryEvent(consumerRecord);
}
```

`processLibraryEvent()` is the Template Method: it deserializes the JSON value back into a (consumer-side) `LibraryEvent` entity, then dispatches by `libraryEventType`:

- **`NEW`** → `save()` — persists `LibraryEvent` + cascaded `Book` via `LibraryEventsRepository` (Spring Data JPA / H2, `@Transactional`).
- **`UPDATE`** → `validate()` then `save()`. `validate()` is where the deliberately-thrown, deliberately-non-retryable failure lives:

```java
private void validate(LibraryEvent libraryEvent) {
    if (libraryEvent.getLibraryEventId() == null) {
        throw new IllegalArgumentException("Library Event Id is missing");
    }
    libraryEventsRepository.findById(libraryEvent.getLibraryEventId())
            .orElseThrow(() -> new IllegalArgumentException("Not a valid library Event: " + ...));
}
```

An `UPDATE` for an id that was never `save()`d as `NEW` (or has a null id) is not a *transient* problem — retrying it will fail identically every time. That's exactly why it's modeled as `IllegalArgumentException` and excluded from retry (below) rather than left to burn through backoff attempts for no benefit.

### Error handling: `LibraryEventsConsumerConfig`

This is the heart of the module's "how do you *actually* handle consumer failures" story. Spring Kafka's `DefaultErrorHandler` wraps a `BackOff` policy and a `ConsumerRecordRecoverer`:

```java
var backOff  = new FixedBackOff(1_000L, 2);              // 1s delay, 2 retries = 3 attempts total
var handler  = new DefaultErrorHandler(recoverer(), backOff);
handler.addNotRetryableExceptions(IllegalArgumentException.class);
```

Recovery strategy (the Strategy-pattern branch inside `recoverer()`), keyed off the **root cause** of the listener exception:

| Root cause | Behavior |
|---|---|
| `RecoverableDataAccessException` | Treated as transient — `LibraryEventsService.handleRecovery()` re-publishes the *same* key/value back onto `library-events` for a fresh pass through the whole pipeline later |
| anything else (including `IllegalArgumentException`, and any exception that survives all retry attempts) | Routed via `DeadLetterPublishingRecoverer` to **`library-events.DLT`**, same partition number as the original record |

Retry attempts are logged via `handler.setRetryListeners(...)`, and the DLT topic is declared explicitly (`NewTopic(DLT_TOPIC).partitions(3).replicas(1)`), so it exists from application startup rather than being created lazily on first dead-letter — matching the source partition count so the "same partition" mapping in the recoverer above is meaningful.

**Retry timeline for a genuinely transient failure** (3 concurrent listener threads, `concurrency=3`, matching the topic's 3 partitions):

```
t=0s      poll → listener throws (e.g. transient DB hiccup)
t=0s+1s   retry #1 (FixedBackOff delay = 1000ms)
t=1s+1s   retry #2 (2 retries configured → 3 total attempts)
t=2s      still failing → recoverer() invoked
             → RecoverableDataAccessException? re-publish to "library-events"
             → anything else?                 → DeadLetterPublishingRecoverer → "library-events.DLT"
```

Only after the recoverer completes does the container commit the offset for that record and move on to the next one — the partition is *not* blocked indefinitely by a poison message, which is the entire point of a dead-letter topic.

### Manual offset commits — `LibraryEventsConsumerManualOffset`

A second, intentionally minimal listener (`@Profile("manual-offset")`, so it never runs alongside the default auto-commit listener) demonstrates the other end of the offset-commit spectrum:

```java
@KafkaListener(topics = {"library-events"})
public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
    acknowledgment.acknowledge();
}
```

With `AcknowledgingMessageListener` + `MANUAL_IMMEDIATE` ack mode, the framework will *not* commit an offset unless the listener calls `acknowledgment.acknowledge()` itself. This is the building block for "commit only after I'm certain the side effect (DB write, downstream call) succeeded" — the default `LibraryEventsConsumer` gets an equivalent effect implicitly (auto-commit only fires after the listener method returns without throwing), but the manual variant makes the commit point an explicit, single line of application code, which matters once processing spans more than one listener invocation (e.g. batching, or committing after a separate async step completes).

---

## Delivery semantics in this module, concretely

Combining the producer and consumer configuration above:

- **Producer → broker**: effectively exactly-once per record, because `acks=all` + `enable.idempotence=true` means a producer retry cannot create a duplicate broker-side write.
- **Broker → consumer**: **at-least-once**. The consumer offset commits only after `processLibraryEvent()` returns (or after the recoverer handles the exception). If the process crashes after `save()` commits to H2 but before the Kafka offset commit lands, the same record is redelivered from the last committed offset on restart, and `processLibraryEvent()` runs again for a record already persisted. The demo does not add an idempotency key/dedup table to fully close this gap — that would be the next step towards effectively-exactly-once consumption.
- **Recovery re-publish**: re-publishing a recovered event creates a *new* record (new offset) on `library-events` — the original failed record's offset is still committed as "handled" (it was handled, by forwarding it). This is why the recoverer, not a retry, is the right tool here: retries reattempt the *existing* record's processing; recovery acknowledges the current attempt is done and defers the real retry to a fresh message.

---

## Running just this module

```bash
# Terminal 1
mvn spring-boot:run -pl kafka-core/library-events-producer

# Terminal 2
mvn spring-boot:run -pl kafka-core/library-events-consumer

# Terminal 3 (optional) — manual-offset demo consumer
mvn spring-boot:run -pl kafka-core/library-events-consumer -Dspring-boot.run.profiles=manual-offset
```

Requires the root `docker-compose.yml` stack running (`docker compose up -d` from the repo root) for Kafka + Kafdrop.

### Inspecting the dead-letter topic

Via Kafdrop (http://localhost:9000) or the CLI:

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic library-events.DLT \
  --from-beginning \
  --property print.key=true
```

### Tests

- `LibraryEventsControllerIntegrationTest` / `LibraryEventControllerUnitTest` / `LibraryEventProducerUnitTest` (producer module) — REST layer and producer unit coverage.
- `LibraryEventsConsumerIntegrationTest` (consumer module) — `@EmbeddedKafka`-backed test that publishes NEW/UPDATE events and asserts the consumer spy, service spy, and H2 repository state, including the "update with unknown id is consumed but not persisted" case that exercises the non-retryable `IllegalArgumentException` path.
- `LibraryEventsConsumerContainerTest` — TestContainers-backed variant.
