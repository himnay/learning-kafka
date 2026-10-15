# Kafka security: SASL/SCRAM over TLS on the host listener

Status: design approved 2026-10-15 — spec for review.
Scope: `kafka-core/docker-compose.yml`, the four client apps (`events-producer`, `events-consumer-a`, `events-consumer-b`, `kafka-stream/orders-streams-app`), their test resources, READMEs.

## 1. What the user asked for

"`sasl.mechanism` and `security.protocol` is not implemented. Can you have a simple username password auth implemented and TLS." Chosen scope: ***SCRAM-SHA-512 + TLS on the host listener***; docker-internal traffic stays plaintext.

## 2. Today

Every listener is `PLAINTEXT`. README §30 only describes how TLS could be enabled.

| Listener | Address | Used by |
|---|---|---|
| `CONTROLLER` | `kafka-N:29093` | KRaft quorum |
| `PLAINTEXT` | `kafka-N:29092` | inter-broker, Schema Registry, Kafka Connect (+ Debezium), Kafdrop, `kafka-init` |
| `PLAINTEXT_HOST` | `localhost:9092/9093/9094` | the four apps started from the IDE / Maven |

## 3. Design

### 3.1 Listeners

| Listener | Protocol | Change |
|---|---|---|
| `CONTROLLER` | `PLAINTEXT` | none |
| `PLAINTEXT` (internal) | `PLAINTEXT` | none — private docker network |
| `EXTERNAL` (was `PLAINTEXT_HOST`) | ***`SASL_SSL`*** | same host ports 9092 / 9093 / 9094; `sasl.enabled.mechanisms=SCRAM-SHA-512`; server-side `ScramLoginModule` (credentials come from the cluster metadata) |

TLS is ***server-authenticated only*** (`ssl.client.auth=none`): the client checks the broker's certificate, the broker checks the client's SCRAM password. Mutual TLS is documented as the alternative, not built.

### 3.2 Certificates (dev CA)

A one-shot `kafka-certs` service (the `cp-kafka` image: it has `keytool`) runs before the brokers and, ***only if the files are missing***, writes to `kafka-core/certs/` (bind mount, git-ignored):

| File | Content |
|---|---|
| `ca.crt` | self-signed dev CA (10 years) |
| `kafka-N.keystore.p12` | broker N's key + certificate signed by the CA; SAN `DNS:localhost, DNS:kafka-N` |
| `kafka.truststore.p12` | the CA certificate — what every client needs |

- Hostname verification stays on (`ssl.endpoint.identification.algorithm=https`, the client default): the brokers advertise `localhost`, which is in every certificate.
- Store passwords are dev constants from compose environment defaults, documented as such.

### 3.3 Users

`kafka-init` creates the SCRAM-SHA-512 credentials through the internal listener (`kafka-configs --alter --add-config 'SCRAM-SHA-512=[password=…]' --entity-type users`), idempotently:

| User | Used by |
|---|---|
| `events-producer` | events-producer |
| `events-consumer` | events-consumer-a and -b |
| `orders-streams` | orders-streams-app |

Authentication only: no authorizer, no ACLs — an authenticated user can still do anything. The README says so and shows what ACLs would add.

### 3.4 Client apps

Every app's `application.yml`:

```yaml
spring:
  kafka:
    security:
      protocol: SASL_SSL
    ssl:
      trust-store-location: file:${KAFKA_TRUSTSTORE:<relative path to kafka-core/certs>/kafka.truststore.p12}
      trust-store-password: ${KAFKA_TRUSTSTORE_PASSWORD:<dev default>}
      trust-store-type: PKCS12
    properties:
      sasl.mechanism: SCRAM-SHA-512
      sasl.jaas.config: org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME:<app user>}" password="${KAFKA_PASSWORD:<dev default>}";
```

- `spring.kafka.security.protocol` and `spring.kafka.ssl.*` apply to producers, consumers, the admin client and Kafka Streams alike.
- The default truststore path is relative to the module directory (the IDE's and Maven's working directory); `KAFKA_TRUSTSTORE` overrides it.
- Schema Registry stays plain HTTP (out of scope).

### 3.5 Tests

Unit and integration tests keep running against plaintext embedded Kafka / Testcontainers brokers. Each module gets `src/test/resources/config/application.yml` with `spring.kafka.security.protocol: PLAINTEXT` (Spring Boot loads `classpath:config/application.yml` on top of `classpath:application.yml`, so only tests see it).

### 3.6 Verification (docker compose, recorded in the README)

1. `openssl s_client -connect localhost:9092 -CAfile kafka-core/certs/ca.crt` → `Verify return code: 0 (ok)`.
2. All four apps start, produce and consume over `SASL_SSL` (library event end to end, coffee order, a Streams query).
3. Wrong password → `SaslAuthenticationException: Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512`.
4. A plaintext client on `localhost:9092` cannot connect.
5. The internal listener keeps working: Schema Registry, Connect (JDBC sink + Debezium), Kafdrop.

## 4. Documentation

- `kafka-core/README.md`: §30 rewritten from "how to enable TLS" to the implemented set-up (listeners, CA, users, client config, verification, how mutual TLS and ACLs would differ); §3 listener table; §18 configuration; §19 running (`kafka-certs`, regenerating certificates).
- Root `README.md` and `kafka-stream/README.md`: client configuration and environment variables.

## 5. Out of scope

- Securing the internal and controller listeners, Schema Registry, Kafka Connect's REST API.
- ACLs / authorization, mutual TLS, OAuth.
