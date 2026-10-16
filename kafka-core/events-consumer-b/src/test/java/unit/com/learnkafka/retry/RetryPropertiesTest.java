package com.learnkafka.retry;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("RetryProperties: app.retry.* durations are validated at startup")
class RetryPropertiesTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    private static RetryProperties properties(Duration interval, Duration maxRun, Duration delay) {
        return new RetryProperties("library-events.retry", "library-events-retry-group", interval, 3, maxRun, delay);
    }

    @Test
    @DisplayName("interval 0s is rejected (it would schedule runs back to back)")
    void zeroInterval_isRejected() {
        assertThat(validator.validate(properties(Duration.ZERO, Duration.ofMinutes(1), Duration.ZERO)))
                .extracting(ConstraintViolation::getPropertyPath).map(Object::toString).contains("interval");
    }

    @Test
    @DisplayName("a negative max-run is rejected (it would silently disable draining)")
    void negativeMaxRun_isRejected() {
        assertThat(validator.validate(properties(Duration.ofSeconds(30), Duration.ofSeconds(-1), Duration.ZERO)))
                .extracting(ConstraintViolation::getPropertyPath).map(Object::toString).contains("maxRun");
    }

    @Test
    @DisplayName("delay 0s is allowed: retry as soon as a run comes")
    void zeroDelay_isAllowed() {
        assertThat(validator.validate(properties(Duration.ofSeconds(30), Duration.ofMinutes(1), Duration.ZERO))).isEmpty();
    }
}
