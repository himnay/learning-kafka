package com.learnkafka.retry;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMin;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * {@code app.retry.*}: where recoverable failures wait, and how the scheduled drain retries them.
 *
 * @param topic       retry topic for library events
 * @param groupId     consumer group of the drain job
 * @param interval    fixed delay between two drain runs
 * @param maxAttempts processing attempts from the retry topic before a record goes to the DLT
 * @param maxRun      a single drain run never lasts longer than this
 * @param delay       how long a record waits on the retry topic before it is retried; defaults to
 *                    {@code interval}. Checked against the record's timestamp (the forward time), so it
 *                    holds across both consumer apps, whose schedules run independently
 */
@Validated
@ConfigurationProperties("app.retry")
public record RetryProperties(
        @NotBlank String topic,
        @NotBlank String groupId,
        @NotNull @DurationMin(millis = 1) Duration interval,
        @Min(1) int maxAttempts,
        @NotNull @DurationMin(millis = 1) Duration maxRun,
        @DurationMin(millis = 0) Duration delay
) {

    public RetryProperties {
        if (delay == null) {
            delay = interval;
        }
    }
}
