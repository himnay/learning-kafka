package com.learnkafka;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real PostgreSQL for the producer's Spring Boot tests. @ServiceConnection points spring.datasource.*
 * at the container, and Flyway creates the coffee schema in it. wal_level=logical lets the outbox test
 * read the write-ahead log the way Debezium does.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgres() {
        return new PostgreSQLContainer("postgres:18").withCommand("postgres", "-c", "wal_level=logical");
    }
}
