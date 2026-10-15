# Library events retry topic — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recoverable failures leave `library-events` for `library-events.retry` with a `retry-attempt` header; a scheduled drain job retries them and dead-letters after `app.retry.max-attempts`.

**Architecture:** One `RetryRouter` (a `ConsumerAwareRecordRecoverer` wrapping two `DeadLetterPublishingRecoverer`s) decides retry-or-DLT for both the main listener's `AfterRollbackProcessor` and the new `RetryTopicDrainer`. The drainer is a `@Scheduled` job with its own short-lived consumer that only processes records below the end offsets it snapshots at the start of a run, one or two Kafka transactions per record.

**Tech Stack:** Java 27, Spring Boot 4.1.1, Spring Kafka 4.1.1, Kafka clients 4.x, Avro + Confluent Schema Registry (mock in tests), PostgreSQL (Testcontainers `postgres:18`), JUnit 5, Mockito, Awaitility.

**Spec:** `docs/superpowers/specs/2026-10-15-library-events-retry-topic-design.md`

## Global Constraints

- Work in `kafka-core/events-consumer-a`; `events-consumer-b` gets byte-identical Java sources and tests (only `application.yml` differs between the two modules) — Task 4.
- Topic `library-events.retry`, 3 partitions; a forwarded record keeps its partition number.
- Header `retry-attempt`: UTF-8 decimal string, `1` on the first hop, replaced (never appended) on each hop.
- Config: `app.retry.topic: library-events.retry`, `group-id: library-events-retry-group`, `interval: 30s`, `max-attempts: 3`, `max-run: 1m`.
- `kafka_dlt-original-*` headers describe the record's position on `library-events` (first hop only); `kafka_dlt-exception-*` describe the latest failure.
- `LibraryEventsService.handleRecovery` is deleted; nothing may re-publish to `library-events`.
- Build/test from `kafka-core`: `mvn -B -pl events-consumer-a -am test` (Docker must be running: Testcontainers PostgreSQL).
- Commit messages: conventional (`feat(kafka-core): …`), ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A record that fails deserialization reaches the retry topic (value `null`): the drainer dead-letters it with its original bytes instead of crashing the run — test in Task 3.
- The forward itself fails (transaction 2: broker down, fenced producer): nothing is committed for that record and the next run sees it again — test in Task 3.
- A record on `library-events` already carrying a `retry-attempt` header (re-driven from the DLT): its retries start again at 1 — test in Task 1.
- A malformed `retry-attempt` value on a retry-topic record: treated as attempt 0, forwarded with 1 — test in Task 1.
- Records forwarded while a run is in progress: processed in the next run, never in the same one — test in Task 3 (one attempt per run).

---

### Task 1: `RetryProperties` and `RetryRouter`

**Files:**
- Create: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/retry/RetryProperties.java`
- Create: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/retry/RetryRouter.java`
- Test: `kafka-core/events-consumer-a/src/test/java/unit/com/learnkafka/retry/RetryRouterTest.java` (the `unit` test source root is already declared in the module's `build-helper-maven-plugin`)

**Interfaces:**
- Produces:
  - `@Validated @ConfigurationProperties("app.retry") public record RetryProperties(@NotBlank String topic, @NotBlank String groupId, @NotNull Duration interval, @Min(1) int maxAttempts, @NotNull Duration maxRun)`
  - `public class RetryRouter implements ConsumerAwareRecordRecoverer`
    - `public static final String ATTEMPT_HEADER = "retry-attempt";`
    - `public enum Route { RETRY, DLT }`
    - `public RetryRouter(KafkaOperations<?, ?> template, RetryProperties properties, String dltTopic)`
    - `public static int attemptOf(ConsumerRecord<?, ?> record)` — last `retry-attempt` header parsed as int; `0` when absent or not a number
    - `public int nextAttempt(ConsumerRecord<?, ?> record)` — `attemptOf(record) + 1` for a record on the retry topic, `1` for a record from any other topic (a stale header is ignored)
    - `public static boolean isRecoverable(Throwable ex)` — `RecoverableDataAccessException` anywhere in the cause chain (move `hasCause` here from `LibraryEventsConsumerConfig`)
    - `public Route route(ConsumerRecord<?, ?> record, Exception ex)` — `RETRY` iff `isRecoverable(ex) && nextAttempt(record) <= properties.maxAttempts()`
    - `public void accept(ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, Exception ex)` — publishes through the retry or the DLT recoverer according to `route`

- [ ] **Step 1: Write the failing tests** — Mockito-mocked `KafkaOperations<Object, Object>` whose `send(any(ProducerRecord.class))` returns a completed `SendResult` future; capture the sent `ProducerRecord`. `RetryProperties("library-events.retry", "library-events-retry-group", Duration.ofSeconds(30), 3, Duration.ofMinutes(1))`, DLT `"library-events.DLT"`, input records on partition `2`; "main record" = topic `library-events`, "retry record" = topic `library-events.retry`.

```java
@Test void firstRecoverableFailure_goesToRetryTopicWithAttemptOne()
    // main record, no header, new RecoverableDataAccessException("db down")
    // sent.topic() == "library-events.retry", sent.partition() == 2
    // values of header "retry-attempt" == ["1"] (exactly one)
@Test void attemptHeaderIsReplacedNotAppended()
    // retry record with retry-attempt "2" → sent to retry topic, header values == ["3"]
@Test void exhaustedAttempts_goToDlt()
    // retry record with "3", max 3 → sent.topic() == "library-events.DLT", partition 2, header values == ["3"]
@Test void nonRecoverable_goesToDltAtOnce()
    // main record, new ListenerExecutionFailedException("x", new IllegalArgumentException("bad")) → DLT, no "retry-attempt" header
@Test void recoverableIsFoundAnywhereInTheCauseChain()
    // new ListenerExecutionFailedException("x", new RuntimeException(new RecoverableDataAccessException("db"))) → route == RETRY
@Test void malformedHeader_countsAsFirstHop()
    // retry record with "abc" → attemptOf == 0, sent to retry with ["1"]
@Test void mainTopicRecordWithStaleHeader_startsAgainAtOne()
    // main record with retry-attempt "3" (re-driven from the DLT), recoverable → sent to retry with ["1"]
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -B -pl events-consumer-a -am test -Dtest=RetryRouterTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation failure — `RetryRouter` / `RetryProperties` do not exist.

- [ ] **Step 3: Implement both classes.** Retry recoverer: `new DeadLetterPublishingRecoverer(template, (r, e) -> new TopicPartition(properties.topic(), r.partition()))`, `setAppendOriginalHeaders(false)`, `setHeadersFunction` returning one `DeadLetterPublishingRecoverer.SingleRecordHeader(ATTEMPT_HEADER, String.valueOf(attemptOf(r) + 1).getBytes(UTF_8))` (replaces the old value). DLT recoverer: same partition on `dltTopic`, `setAppendOriginalHeaders(false)`. Keep the existing log lines ("Routing to DLT: key={} cause={}", plus "Forwarding to {} attempt={}: key={}").

- [ ] **Step 4: Run to verify they pass** (same command). Expected: `Tests run: 7, Failures: 0`.

- [ ] **Step 5: Commit** — `feat(kafka-core): RetryRouter decides retry topic or DLT by attempt header`

### Task 2: Main listener forwards to the retry topic; `handleRecovery` removed

**Files:**
- Modify: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/config/LibraryEventsConsumerConfig.java` (recoverer, new beans, drop the `LibraryEventsService` field)
- Modify: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/service/LibraryEventsService.java` (delete `handleRecovery` and its now-unused imports)
- Modify: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/EventsConsumerApplication.java` (`@EnableScheduling`, `@ConfigurationPropertiesScan`)
- Modify: `kafka-core/events-consumer-a/src/main/resources/application.yml` (new `app.retry` block, values from Global Constraints, with one-line comments)
- Modify: `kafka-core/docker-compose.yml` (`kafka-init` loop: `for t in library-events library-events.retry library-events.DLT key-routing`)
- Test: `kafka-core/events-consumer-a/src/test/java/intg/com/learnkafka/retry/RetryTopicIntegrationTest.java` (new)

**Interfaces:**
- Consumes: `RetryRouter`, `RetryProperties` (Task 1).
- Produces: `@Bean RetryRouter retryRouter()` (built with the app's `KafkaTemplate<Integer, Object>`, `RetryProperties`, `LibraryEventsConsumerConfig.DLT_TOPIC`); `@Bean NewTopic libraryEventsRetry()` — `TopicBuilder.name(retryProperties.topic()).partitions(3).build()`.

- [ ] **Step 1: Write the failing test.** `RetryTopicIntegrationTest` copies the class annotations of `LibraryEventsConsumerIntegrationTest` (`@SpringBootTest`, `@Import(TestcontainersConfiguration.class)`, the same `@EmbeddedKafka` with its four broker properties, the same four `@TestPropertySource` properties) and adds `"app.retry.interval=1h"` so the schedule never fires. `@MockitoSpyBean LibraryEventsService serviceSpy`. Before each test, wait for listener assignment as `LibraryEventsConsumerIntegrationTest.setUp` does. Helper `List<ConsumerRecord<byte[], byte[]>> readAll(String topic)` — fresh consumer, `earliest`, polls until two empty polls. Events are NEW library events built with `AvroEvents.fromJson`, each test with its own book name and id; stubs match on the book name.

```java
@Test void recoverableFailure_isForwardedToRetryTopicOnce()
    // doThrow(new RecoverableDataAccessException("database down")).when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Once")))
    // kafkaTemplate.executeInTransaction(t -> t.sendDefault(9101, event))
    // await: library-events.retry holds a record decoding (AvroEvents.decode) to event
    //   header "retry-attempt" values == ["1"]; header "kafka_dlt-original-topic" == "library-events"
    // readAll("library-events") has exactly 1 record decoding to event      (no re-publish loop)
    // verify(serviceSpy, times(3)).processLibraryEvent(argThat(isBook("Retry-Once")))   (1 + 2 in-place retries)
```

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -B -pl events-consumer-a -am test -Dtest=RetryTopicIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL — no record on `library-events.retry` (today the record is re-published to `library-events`).

- [ ] **Step 3: Implement.** `afterRollbackProcessor()` passes `retryRouter()` as the recoverer to `DefaultAfterRollbackProcessor` (keep `FixedBackOff(1_000L, 2)`, `commitRecovered=true`, the two not-retryable exceptions). Delete `recoverer()` and `hasCause`. Update the class Javadoc's recovery bullets to "retry topic / DLT".

- [ ] **Step 4: Run the module's whole suite**

Run: `mvn -B -pl events-consumer-a -am test`
Expected: BUILD SUCCESS; the existing DLT tests in `LibraryEventsConsumerIntegrationTest` still pass.

- [ ] **Step 5: Commit** — `feat(kafka-core): recoverable failures go to library-events.retry instead of looping`

### Task 3: `RetryTopicDrainer`

**Files:**
- Create: `kafka-core/events-consumer-a/src/main/java/com/learnkafka/retry/RetryTopicDrainer.java`
- Test: `kafka-core/events-consumer-a/src/test/java/intg/com/learnkafka/retry/RetryTopicIntegrationTest.java` (add tests)

**Interfaces:**
- Consumes: `RetryRouter` (`route`, `accept`, `ATTEMPT_HEADER`), `RetryProperties`, `LibraryEventsService.processLibraryEvent(ConsumerRecord<Integer, LibraryEventAvro>)`, `EventMetadataMdcInterceptor.put/clear`, `LibraryEventsConsumer.AVRO_READER_TYPE`.
- Produces:
  - `public record DrainResult(int processed, int requeued, int deadLettered)` (nested in `RetryTopicDrainer`)
  - `public DrainResult drainOnce()`
  - `@Scheduled(fixedDelayString = "${app.retry.interval}", initialDelayString = "${app.retry.interval}") public void scheduledDrain()` — calls `drainOnce()`
  - `@PreDestroy public void stop()`

- [ ] **Step 1: Write the failing tests** (in `RetryTopicIntegrationTest`, `@Autowired RetryTopicDrainer drainer`):

```java
@Test void drainRun_succeeds_onceTheDatabaseRecovers()
    // doThrow(rdae).doThrow(rdae).doThrow(rdae).doCallRealMethod().when(serviceSpy).processLibraryEvent(argThat(isBook("Retry-Recovers")))
    // send; await the retry record (attempt "1"); drainer.drainOnce()
    // libraryEventsRepository.findAll() contains a book named "Retry-Recovers"
    // drainer.drainOnce() again → verify(serviceSpy, times(4)).processLibraryEvent(argThat(isBook("Retry-Recovers")))
@Test void alwaysFailing_isDeadLetteredAfterMaxAttempts_oneAttemptPerRun()
    // stub always throws rdae for "Retry-Never"; send; await attempt "1"
    // run 1: drainOnce → exactly one more call for the book; retry topic gains a record for the event with attempt "2"
    // run 2: → one more call; attempt "3"
    // run 3: → one more call; library-events.DLT holds the event with "retry-attempt" == "3",
    //        "kafka_dlt-exception-fqcn" == RecoverableDataAccessException's name, "kafka_dlt-original-topic" == "library-events"
@Test void undeserializableRecordOnRetryTopic_isDeadLetteredWithOriginalBytes()
    // produce raw bytes "not avro".getBytes() with a ByteArraySerializer directly to library-events.retry (header retry-attempt "1")
    // drainOnce → library-events.DLT holds a record whose value equals those bytes
@Test void failedForward_leavesTheRecordForTheNextRun()
    // @MockitoSpyBean RetryRouter routerSpy; service always throws rdae for "Retry-Forward-Fails"
    // doThrow(new KafkaException("broker down")).doCallRealMethod().when(routerSpy).accept(argThat(isBook(...)), any(), any())
    // send; await attempt "1"; drainOnce → no attempt-"2" record yet
    // drainOnce again → the service is called for the book again and the retry topic gains attempt "2"
```

- [ ] **Step 2: Run to verify they fail** (command from Task 2). Expected: compilation failure — `RetryTopicDrainer` does not exist.

- [ ] **Step 3: Implement `drainOnce()`** with this algorithm (the parts the signature doesn't decide):

```text
consumer = consumerFactory.createConsumer(groupId, "retry-drainer", null, props{specific.avro.value.type=com.learnkafka.avro.LibraryEventAvro})
subscribe([topic], rebalance listener: onPartitionsRevoked → active.removeAll(revoked))
poll(200 ms) until assignment non-empty, at most 10 s; none → close, return DrainResult(0,0,0)
for tp in assignment: committed offset → seek(tp, offset), else seekToBeginning(tp)   # discard records fetched while waiting
end = endOffsets(assignment); active = {tp | position(tp) < end[tp]}
deadline = now + maxRun
while active non-empty and now < deadline and not stopping:
    for record in poll(500 ms):
        tp = record's partition
        if tp not in active: continue
        if record.offset() >= end[tp]: active.remove(tp); continue
        handle(record)                      # below
    active.removeIf(tp -> position(tp) >= end[tp])
close consumer; log "Retry drain: processed={} requeued={} deadLettered={} in {} ms"

handle(record):
    MDC from EventMetadata.of(record.value())
    try   executeInTransaction: service.processLibraryEvent(record); sendOffsetsToTransaction({tp: offset+1}, consumer.groupMetadata())  → processed++
    catch ex: route = router.route(record, ex)
              executeInTransaction: router.accept(record, consumer, ex); sendOffsetsToTransaction(same)  → requeued++ or deadLettered++
    finally MDC clear
a failure of the second transaction propagates: drainOnce logs it and returns the counts so far (nothing committed for that record)
```

- [ ] **Step 4: Run the module's whole suite** — `mvn -B -pl events-consumer-a -am test`. Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit** — `feat(kafka-core): scheduled drain of library-events.retry with max attempts`

### Task 4: consumer-b, documentation, end-to-end check

**Files:**
- Copy to `kafka-core/events-consumer-b/src/...` every file created or changed under `kafka-core/events-consumer-a/src/` in Tasks 1–3, except `application.yml`; apply the same `app.retry` block to `kafka-core/events-consumer-b/src/main/resources/application.yml`.
- Modify: `kafka-core/README.md` — §14 rewritten around the new flow (mermaid: main topic → in-place retries → `library-events.retry` → scheduled drain → DLT; header table from spec §3.2; trade-off from spec §3.8 with a pointer to §23); every "re-publish" mention (currently lines 599, 659, 666, 687, 746–747); §18 configuration (`app.retry.*`); the `kafka-init` topic list (line 815 table row); §31 test counts.
- Modify: `README.md` (root) — "re-publish" mentions (lines 109, 139, 194, 235, 445), test counts.

- [ ] **Step 1: Mirror and verify identical sources**

Run: `diff -rq kafka-core/events-consumer-a/src kafka-core/events-consumer-b/src`
Expected: only `application.yml` differs.

- [ ] **Step 2: Run all kafka-core tests**

Run: `cd kafka-core && mvn -B test`
Expected: BUILD SUCCESS for every module.

- [ ] **Step 3: End to end on docker compose.** Recreate `kafka-init` (`docker compose up -d kafka-init`); start the producer and both consumers; stop PostgreSQL (`docker stop kafka-core-postgres`), POST a library event, wait for the forward (`kafka-console-consumer … --topic library-events.retry --property print.headers=true` shows `retry-attempt:1`), start PostgreSQL, wait one interval → the book is in the `library_event` table. Record the commands and output in §14.

- [ ] **Step 4: Update the docs listed above**, then `git diff --stat` shows only the files listed in this task.

- [ ] **Step 5: Commit and push** — `docs(kafka-core): retry topic and scheduled drain; mirror into consumer-b`, then `git push origin main`.
