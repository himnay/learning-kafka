package com.learnkafka.coffee;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Refuses to start when coffee orders could be accepted but never published.
 *
 * Debezium reads the outbox from the write-ahead log through its replication slot. The outbox row is
 * deleted in the same transaction, so an order committed while PostgreSQL has no slot (or runs with
 * wal_level below logical) exists only in coffee_order: the event is gone for good. Runs while the
 * context starts, before the web server accepts requests.
 *
 * docker compose creates the slot as soon as PostgreSQL is up (outbox-slot-init); tests and ad-hoc runs
 * without Debezium set app.coffee.require-outbox-slot=false to get a warning instead.
 */
@Slf4j
@Component
public class OutboxCaptureCheck implements InitializingBean {

    enum Outcome { OK, WAL_NOT_LOGICAL, SLOT_MISSING }

    private final JdbcTemplate jdbcTemplate;
    private final String slot;
    private final boolean required;

    public OutboxCaptureCheck(JdbcTemplate jdbcTemplate,
                              @Value("${app.coffee.outbox-slot}") String slot,
                              @Value("${app.coffee.require-outbox-slot:true}") boolean required) {
        this.jdbcTemplate = jdbcTemplate;
        this.slot = slot;
        this.required = required;
    }

    static Outcome decide(String walLevel, boolean slotExists) {
        if (!"logical".equals(walLevel)) {
            return Outcome.WAL_NOT_LOGICAL;
        }
        return slotExists ? Outcome.OK : Outcome.SLOT_MISSING;
    }

    @Override
    public void afterPropertiesSet() {
        String walLevel = jdbcTemplate.queryForObject("SHOW wal_level", String.class);
        boolean slotExists = Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = ?)", Boolean.class, slot));
        Outcome outcome = decide(walLevel, slotExists);
        if (outcome == Outcome.OK) {
            log.info("Coffee outbox is captured: wal_level=logical, replication slot {} exists", slot);
            return;
        }
        String problem = switch (outcome) {
            case WAL_NOT_LOGICAL -> "PostgreSQL runs with wal_level=" + walLevel + " (Debezium needs logical)";
            case SLOT_MISSING -> "replication slot " + slot + " does not exist";
            case OK -> throw new IllegalStateException("unreachable");
        };
        String consequence = problem + ": coffee orders would be accepted but their events never published."
                + " Run `docker compose up -d` in kafka-core (it sets wal_level and creates the slot)"
                + " or set app.coffee.require-outbox-slot=false.";
        if (required) {
            throw new IllegalStateException(consequence);
        }
        log.warn(consequence);
    }
}
