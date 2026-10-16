package com.learnkafka.coffee;

import com.learnkafka.coffee.OutboxCaptureCheck.Outcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("OutboxCaptureCheck: will Debezium see the orders written from now on?")
class OutboxCaptureCheckTest {

    @Test
    @DisplayName("wal_level=logical and the slot exists: every outbox insert is kept until Debezium reads it")
    void logicalWithSlot_ok() {
        assertThat(OutboxCaptureCheck.decide("logical", true)).isEqualTo(Outcome.OK);
    }

    @Test
    @DisplayName("wal_level=replica: nothing can be decoded, whatever slots exist")
    void notLogical() {
        assertThat(OutboxCaptureCheck.decide("replica", true)).isEqualTo(Outcome.WAL_NOT_LOGICAL);
    }

    @Test
    @DisplayName("No slot yet: orders written now are gone before Debezium starts reading")
    void slotMissing() {
        assertThat(OutboxCaptureCheck.decide("logical", false)).isEqualTo(Outcome.SLOT_MISSING);
    }
}
