# Library events: retry topic with a scheduled drain

Status: design approved 2026-10-15 — spec for review.
Scope: `kafka-core/events-consumer-a` and `kafka-core/events-consumer-b` (identical Java code; only `application.yml` differs), `kafka-core/docker-compose.yml`, READMEs.

## 1. What the user asked for

- A record that fails with a ***recoverable*** error must not be re-published to its own topic. It goes to a ***different topic*** (a retry topic) with a ***counter header*** that is incremented on every hop.
- The consumer of the retry topic checks the counter; when it reaches a ***configurable*** maximum, the record goes to the ***DLQ***.
- Retries must not hold records in the application: "if it holds and the application crash or restart than it all starts again". Move the record to the retry topic ***immediately*** and let a ***scheduler*** retry it according to its configuration.

## 2. Today, and the bug this replaces

- Main listener: transactional container (`KafkaTransactionManager`), `DefaultAfterRollbackProcessor` with `FixedBackOff(1 s, 2)`. After the in-place attempts the recoverer either dead-letters the record (`library-events.DLT`) or, for `RecoverableDataAccessException`, calls `LibraryEventsService.handleRecovery`.
- `handleRecovery` re-publishes `(key, value)` to `library-events` with `sendDefault`: it ***drops all headers*** and has ***no attempt limit***. While the database is down, the record loops forever.

## 3. Design

### 3.1 Topics

| Topic | Partitions | Written by | Read by |
|---|---|---|---|
| `library-events` | 3 | events-producer | main listener (unchanged) |
| `library-events.retry` ***(new)*** | 3 | main recoverer, retry drainer | `RetryTopicDrainer` |
| `library-events.DLT` | 3 | main recoverer, retry drainer | nobody (inspection) |

- `library-events.retry` is created by `kafka-init` (RF 3, `min.insync.replicas=2`, like the others) and declared as a `NewTopic` bean, so the apps also work against a bare broker.
- A forwarded record keeps its ***partition number*** (same as the DLT today), so per-key order is kept ***within*** the retry topic.

### 3.2 Headers

| Header | Meaning |
|---|---|
| `retry-attempt` | how many times the record has been forwarded to the retry topic: `1` on the first hop, then `n + 1`. UTF-8 decimal string (readable in Kafdrop). Replaced, never appended, on each hop. |
| producer headers | kept as they were on the original record |
| `kafka_dlt-exception-*` | Spring's diagnostics for the ***latest*** failure (class, message, stack trace) |
| `kafka_dlt-original-*` | topic / partition / offset / timestamp of the record on `library-events` |

The DLT record carries the final `retry-attempt` value, so you can tell "dead-lettered at once" (no header) from "dead-lettered after N retries".

Only a record read from the retry topic counts its header: a record on `library-events` that already carries `retry-attempt` (re-driven from the DLT) starts again at 1.

### 3.3 Main listener (changed recoverer only)

- In-place retries stay: `FixedBackOff(1 s, 2)` absorbs blips without leaving the partition.
- Recoverer, after the in-place attempts:
  - cause chain contains `RecoverableDataAccessException` → forward to `library-events.retry` with `retry-attempt: 1`;
  - anything else (`IllegalArgumentException`, `DataIntegrityViolationException`, deserialization failures, …) → `library-events.DLT`, as today.
- The forward and the consumed offset commit in ***one Kafka transaction*** (`commitRecovered=true`, unchanged).
- `LibraryEventsService.handleRecovery` is deleted.

### 3.4 `RetryTopicDrainer` (new, `com.learnkafka.retry`)

Triggered by `@Scheduled(fixedDelayString = "${app.retry.interval}")` (`@EnableScheduling` added). One run:

1. Create a consumer from Boot's `ConsumerFactory` (same deserializers: `ErrorHandlingDeserializer` → `KafkaAvroDeserializer`, `read_committed`, the pinned Avro reader type) with group `library-events-retry-group` and its own client id. Subscribe to `library-events.retry`.
2. Poll until partitions are assigned (bounded wait). No assignment (the other app holds them all) → end the run.
3. ***Snapshot*** `endOffsets(assignment)`. Only records ***below*** the snapshot are processed in this run. A record forwarded during the run lands above it and waits for the next run, so attempts are at least one `interval` apart.
4. Poll and process records one by one (3.5) until every assigned partition's position has reached its snapshot, or `app.retry.max-run` has elapsed, or the app is shutting down.
5. Close the consumer. Partitions revoked mid-run (the other app joined) are dropped from the run.

Runs never overlap inside one app (single scheduler thread, fixed delay). Both apps run the drainer in the same group, so the retry partitions are split between them like the main topic's.

### 3.5 One record

Two Kafka transactions at most, on the app's transactional `KafkaTemplate` (`executeInTransaction`):

1. ***Transaction 1*** — `LibraryEventsService.processLibraryEvent(record)` (its own database transaction, plus the `library-book-state` send, which joins the Kafka transaction), then `sendOffsetsToTransaction(offset + 1, consumer.groupMetadata())`. Commit.
2. If processing throws, transaction 1 ***aborts*** — a state record already sent in it is discarded. Then ***transaction 2***:
   - recoverable (`RecoverableDataAccessException` in the cause chain) and `retry-attempt < app.retry.max-attempts` → forward to `library-events.retry` with `retry-attempt + 1`;
   - otherwise (non-recoverable, or attempts exhausted, or a deserialization failure) → `library-events.DLT`;
   - plus the offset. Commit.
3. If transaction 2 itself fails (Kafka unreachable), the run stops; nothing was committed for that record, so the next run reads it again.

Crash at any point: the next run resumes at the last committed offset. A record whose database transaction committed but whose Kafka transaction did not is processed again and skipped by the existing idempotent consumer (`processed_event`, unique on `consumer_group, event_id`).

Logs of a drained record carry the same MDC fields as the main listener (event type and id). Each run logs one summary line: processed / re-queued / dead-lettered / duration.

### 3.6 Shared routing

The "retry topic or DLT, and with which attempt number" decision and the two `DeadLetterPublishingRecoverer`s (retry destination, DLT destination) live in one class used by both the main recoverer and the drainer, so the rules can't drift apart.

### 3.7 Configuration

```yaml
app:
  retry:
    topic: library-events.retry
    group-id: library-events-retry-group
    interval: 30s       # fixed delay between drain runs
    max-attempts: 3     # processing attempts from the retry topic before the DLT
    max-run: 1m         # a single run never drains longer than this
```

Validated at startup (`max-attempts >= 1`, positive durations).

### 3.8 Trade-off (documented)

While a record waits in the retry topic, newer events with the same key are processed from the main topic: per-key ***order is not kept across the two topics*** (an UPDATE can overtake its NEW). The README spells this out next to the coffee orders' `@RetryableTopic` (§23), which has the same trade-off but delays by pausing partitions until a due time.

## 4. Testing

- ***Unit*** — the routing decision: first hop → retry with attempt 1; attempt 2 of 3 → retry with 3; attempt 3 of 3 → DLT; non-recoverable at any attempt → DLT; missing or malformed header.
- ***Integration*** (existing consumer test set-up: embedded Kafka + Testcontainers PostgreSQL + mock Schema Registry; `app.retry.interval` set to an hour so the schedule never fires and the test calls the drain run directly):
  1. a recoverable failure on `library-events` ends on `library-events.retry` with `retry-attempt=1` and the original headers; nothing on the DLT; `library-events` still holds exactly one record (no loop);
  2. the next drain run succeeds once the database recovers → the book is in PostgreSQL and a further run finds nothing to do;
  3. a record that always fails → `retry-attempt` 1, 2, 3 across three runs, then the DLT with `retry-attempt=3` and exception headers;
  4. a record re-queued during a run is not processed again in that run.
- Existing tests that exercised `handleRecovery` are replaced by the above. Both consumer modules get the same tests.

## 5. Documentation

- `kafka-core/README.md`: §14 (error handling) rewritten around the new flow, with a diagram; §18 configuration; `kafka-init` topic list; §31 test counts; the trade-off of 3.8.
- Root `README.md`: test counts, any topic lists.

## 6. Out of scope

- Growing delays / several retry topics (rejected in favour of one topic and a fixed schedule).
- Re-driving records from the DLT.
- Changing the coffee orders' `@RetryableTopic` setup.
