# Kafka SASL/SCRAM over TLS (host listener) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apps on the host connect to the 3-broker cluster with `SASL_SSL` + `SCRAM-SHA-512` (per-app users, generated dev CA); docker-internal traffic stays plaintext.

**Architecture:** A one-shot `kafka-certs` service (keytool in the `cp-kafka` image) writes a dev CA, one keystore per broker and a client truststore into `kafka-core/certs/` (git-ignored). The host listener is renamed `PLAINTEXT_HOST` → `EXTERNAL` and becomes `SASL_SSL`, configured with `listener.name.external.*` properties. `kafka-init` creates the SCRAM users over the internal listener. Each app gets `spring.kafka.security/ssl/properties`; the Streams app merges them into its own `KafkaStreamsConfiguration`. Tests keep plaintext through `src/test/resources/config/application.yml`.

**Tech Stack:** cp-kafka 8.3.2 (KRaft), keytool (PKCS12), Spring Boot 4.1.1 `KafkaProperties`, Kafka Streams, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-10-15-sasl-scram-tls-design.md`

## Global Constraints

- Only the host listener changes: name `EXTERNAL`, protocol `SASL_SSL`, mechanism `SCRAM-SHA-512`, host ports 9092 / 9093 / 9094 unchanged. `CONTROLLER` and `PLAINTEXT` (`kafka-N:29092`) stay `PLAINTEXT`.
- The listener name must not contain `SSL` or `SASL_`: the cp image's configure script then applies its own file-based secrets checks (`KAFKA_SSL_KEYSTORE_FILENAME`, `KAFKA_OPTS` with a JAAS file). `EXTERNAL` avoids both; everything is set through `KAFKA_LISTENER_NAME_EXTERNAL_*` (cp env mapping: `_` → `.`, `__` → `_`, `___` → `-`).
- TLS is server-authenticated only (`ssl.client.auth=none`); hostname verification stays on — every broker certificate has SAN `DNS:localhost, DNS:kafka-N`.
- Files in `kafka-core/certs/` (git-ignored, generated only if missing): `ca.crt`, `ca.p12` (CA key, dev only), `kafka-1.keystore.p12`, `kafka-2.keystore.p12`, `kafka-3.keystore.p12`, `kafka.truststore.p12`. Store/key password `${KAFKA_SSL_PASSWORD:-kafka-dev-secret}`.
- SCRAM users and dev default passwords: `events-producer` / `events-producer-secret`, `events-consumer` / `events-consumer-secret`, `orders-streams` / `orders-streams-secret`. Apps read `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `KAFKA_TRUSTSTORE`, `KAFKA_TRUSTSTORE_PASSWORD`, with those defaults.
- Default truststore path is relative to the module directory: `../certs/kafka.truststore.p12` in kafka-core apps, `../../kafka-core/certs/kafka.truststore.p12` in `orders-streams-app`.
- No authorizer, no ACLs.
- The user's uncommitted edits (`events-producer` and `events-consumer-b` `application.yml`, a `docker-compose.yml` comment) stay unstaged: stage only this plan's hunks.
- Do not recreate the user's running compose stack: end-to-end checks run on a throwaway single-node broker; the user applies the change with `docker compose up -d`.
- Commit messages conventional, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; push to `main` after each task.

## Review Focus

- A client with the wrong password: `SaslAuthenticationException` (… invalid credentials with SASL mechanism SCRAM-SHA-512), not a hang — e2e step in Task 3.
- A plaintext client on `localhost:9092`: the connection fails instead of silently working — e2e step in Task 3.
- `kafka-certs` run twice: existing certificates are kept (brokers keep trusting the same CA) — e2e step in Task 1.
- Internal listener clients (Schema Registry, Kafka Connect + Debezium, Kafdrop, `kafka-init`) keep working unchanged — covered by the throwaway-broker check of the internal listener in Task 1.
- Tests in CI where `kafka-core/certs/` does not exist: every module's suite still passes (the test-only config sets `PLAINTEXT`) — Task 2 runs all suites.

---

### Task 1: Certificates, the `EXTERNAL` listener and SCRAM users (docker compose)

**Files:**
- Create: `kafka-core/certs/.gitignore` (`*` and `!.gitignore`) — the directory must exist and belong to the user, or docker creates it as root and `kafka-certs` (uid 1000) can't write
- Modify: `kafka-core/docker-compose.yml`:
  - new service `kafka-certs` (image `confluentinc/cp-kafka:8.3.2`, `entrypoint: ["/bin/sh", "-c"]`, bind mount `./certs:/certs`, `restart: "no"`, env `CERT_PASSWORD: ${KAFKA_SSL_PASSWORD:-kafka-dev-secret}`): if `/certs/kafka.truststore.p12` exists, exit 0; else keytool — CA keypair (`-ext bc:c`, 3650 days) in `ca.p12`, export `ca.crt`, import it into `kafka.truststore.p12`; for N in 1..3: keypair `kafka-N` with `-ext SAN=dns:localhost,dns:kafka-N`, `-certreq`, `-gencert` by the CA (same SAN, `eku=serverAuth`), import CA then signed cert into `kafka-N.keystore.p12`. Every `$` in the script is `$$` (compose interpolation).
  - shared broker env (`&kafka-env`): `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:SASL_SSL`; `KAFKA_LISTENER_NAME_EXTERNAL_SASL_ENABLED_MECHANISMS: SCRAM-SHA-512`; `KAFKA_LISTENER_NAME_EXTERNAL_SCRAM___SHA___512_SASL_JAAS_CONFIG: org.apache.kafka.common.security.scram.ScramLoginModule required;`; `KAFKA_LISTENER_NAME_EXTERNAL_SSL_KEYSTORE_TYPE: PKCS12`; keystore and key password from `${KAFKA_SSL_PASSWORD:-kafka-dev-secret}`
  - per broker N: `KAFKA_LISTENERS: PLAINTEXT://kafka-N:29092,CONTROLLER://kafka-N:29093,EXTERNAL://0.0.0.0:909x`, `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-N:29092,EXTERNAL://localhost:909x`, `KAFKA_LISTENER_NAME_EXTERNAL_SSL_KEYSTORE_LOCATION: /etc/kafka/secrets/kafka-N.keystore.p12`, volume `./certs:/etc/kafka/secrets:ro`, `depends_on: kafka-certs: { condition: service_completed_successfully }`
  - `kafka-init`: after the topics, `kafka-configs --bootstrap-server kafka-1:29092 --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=…]' --entity-type users --entity-name <user>` for the three users (passwords `${EVENTS_PRODUCER_KAFKA_PASSWORD:-events-producer-secret}` etc.); re-running only replaces the credentials

**Interfaces:**
- Produces: host listener `localhost:9092/9093/9094` = `SASL_SSL` + `SCRAM-SHA-512`; truststore `kafka-core/certs/kafka.truststore.p12`; users above.

- [ ] **Step 1: Validate the compose file** — `cd kafka-core && docker compose config --quiet`. Expected: no output, exit 0.
- [ ] **Step 2: Generate the certificates with the real service** — `docker compose up --no-deps kafka-certs`. Expected: exit 0; `kafka-core/certs/` holds the six files; `keytool -list -v -keystore certs/kafka-1.keystore.p12 -storepass kafka-dev-secret` (inside the cp-kafka image) shows `DNSName: localhost` and `DNSName: kafka-1`, issuer `CN=kafka-core dev CA`. Run it again: "certificates already exist", files unchanged (`sha256sum` before/after).
- [ ] **Step 3: Throwaway broker with the same listener settings** — one `cp-kafka:8.3.2` KRaft node in a throwaway container (its own name, host port 19092 and `CLUSTER_ID`) with the shared env above, `EXTERNAL://0.0.0.0:19092` advertised as `localhost:19092`, keystore `kafka-1.keystore.p12` mounted read-only. Create a SCRAM user over its internal listener (same `kafka-configs` command). Expected: `openssl s_client -connect localhost:19092 -CAfile kafka-core/certs/ca.crt </dev/null` → `Verify return code: 0 (ok)` (openssl on the host); `kafka-broker-api-versions` over the internal listener works.
- [ ] **Step 4: Commit and push** — `feat(kafka-core): SASL/SCRAM over TLS on the host listener (compose)`; stage only this task's hunks of `docker-compose.yml`.

### Task 2: Clients use SASL_SSL; tests stay plaintext

**Files:**
- Modify: `kafka-core/events-producer/src/main/resources/application.yml`, `kafka-core/events-consumer-a/src/main/resources/application.yml`, `kafka-core/events-consumer-b/src/main/resources/application.yml`, `kafka-stream/orders-streams-app/src/main/resources/application.yml` — under `spring.kafka`: `security.protocol: SASL_SSL`; `ssl.trust-store-location: file:${KAFKA_TRUSTSTORE:<module default>}`, `ssl.trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD:kafka-dev-secret}`, `ssl.trust-store-type: PKCS12`; `properties.sasl.mechanism: SCRAM-SHA-512`; `properties.sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME:<app user>}" password="${KAFKA_PASSWORD:<app password>}";` (consumer-a and -b both use `events-consumer`)
- Create: `src/test/resources/config/application.yml` in the same four modules — `spring.kafka.security.protocol: PLAINTEXT` (Boot loads `classpath:config/application.yml` on top of `classpath:application.yml`)
- Modify: `kafka-stream/orders-streams-app/src/main/java/com/learnkafkastreams/config/OrdersStreamsConfiguration.java` — `kStreamConfig` also takes Boot's `KafkaProperties` (and `SslBundles` if the 4.1.1 `Ssl.buildProperties` signature needs it) and merges `kafkaProperties.getProperties()`, the security properties and the SSL properties into its map, before its own settings
- Test: `kafka-stream/orders-streams-app/src/test/java/.../OrdersStreamsConfigurationTest.java`

**Interfaces:**
- Consumes: Task 1's user names, passwords and truststore path.
- Produces: `kStreamConfig(...)` returns a `KafkaStreamsConfiguration` whose properties include `security.protocol`, `sasl.mechanism`, `sasl.jaas.config` and `ssl.truststore.location` whenever they are configured.

- [ ] **Step 1: Write the failing test** (`OrdersStreamsConfigurationTest`):

```java
@Test void securitySettingsReachKafkaStreams()
    // KafkaProperties with security.protocol=SASL_SSL, properties sasl.mechanism=SCRAM-SHA-512 and
    // sasl.jaas.config="…ScramLoginModule required username=\"orders-streams\" password=\"x\";",
    // ssl trust-store-location=file:/tmp/truststore.p12, type PKCS12
    // → kStreamConfig(...).asProperties() has security.protocol "SASL_SSL", sasl.mechanism "SCRAM-SHA-512",
    //   the same sasl.jaas.config, and ssl.truststore.location ending in "truststore.p12"
@Test void plaintextByDefault()
    // empty KafkaProperties → no "sasl.mechanism" key, and security.protocol absent or "PLAINTEXT"
```

- [ ] **Step 2: Run to verify it fails** — `mvn -B -f kafka-stream/pom.xml -pl orders-streams-app -am test -Dtest=OrdersStreamsConfigurationTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: compilation failure or `security.protocol` missing.
- [ ] **Step 3: Implement the merge, the four `application.yml` changes and the four test-only configs.**
- [ ] **Step 4: Run every suite** — `mvn -B -f kafka-core/pom.xml test` and `mvn -B -f kafka-stream/pom.xml test`. Expected: BUILD SUCCESS for both (tests use plaintext embedded / Testcontainers brokers).
- [ ] **Step 5: Commit and push** — `feat: apps connect with SASL_SSL + SCRAM-SHA-512; tests stay plaintext`; stage only this task's hunks of the two user-edited `application.yml` files.

### Task 3: End-to-end on the throwaway broker, docs

**Files:**
- Modify: `kafka-core/README.md` — §30 rewritten from "Enabling TLS" into what is implemented (listeners table, `kafka-certs` and the files, SCRAM users, client config and env vars, verification commands with the real output from this task, how mutual TLS and ACLs would differ); §3 listener table; §18 configuration; §19 running (`kafka-certs`, regenerating = delete `certs/*` except `.gitignore`, then `docker compose up -d`)
- Modify: `README.md` (root) and `kafka-stream/README.md` — client security settings and env vars

- [ ] **Step 1: Run the producer and the Streams app against the throwaway broker** (users created as in Task 1 Step 3):
  - `java -jar events-producer/target/*.jar --server.port=18080 --spring.kafka.bootstrap-servers=localhost:19092`, then `POST /v1/key-routing`. Expected: 201.
  - `orders-streams-app` with `KAFKA_BOOTSTRAP_SERVERS=localhost:19092`, `KAFKA_USERNAME=orders-streams`, `--server.port=18082`. Expected: the log reaches `State transition from REBALANCING to RUNNING` (its security settings come from the merge in Task 2, not from Boot).
  - The consumer apps use the same Boot-managed client settings as the producer, so the producer run covers that path; they need PostgreSQL and are checked when the user applies the change to the real stack.
- [ ] **Step 2: Wrong password** — same app with `KAFKA_PASSWORD=wrong`. Expected: startup/send fails with `SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`.
- [ ] **Step 3: Plaintext client** — `kafka-broker-api-versions --bootstrap-server localhost:19092` without security settings (from a cp-kafka container on the host network). Expected: it cannot connect (disconnects during handshake).
- [ ] **Step 4: Tear down** the throwaway broker and app; write the docs with the real output from Steps 1–3 and Task 1.
- [ ] **Step 5: Commit and push** — `docs(kafka-core): SASL/SCRAM over TLS`.
