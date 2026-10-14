package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Maps domain exceptions to RFC 9457 ProblemDetail responses.
 * Requires spring.mvc.problemdetails.enabled: true in application.yml.
 */
@Slf4j
@RestControllerAdvice
public class OrdersExceptionHandler {

    /** Handles bad request. */
    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException ex) {
        log.warn("Bad request: {}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    /**
     * The state stores are not queryable yet — Kafka Streams is starting or rebalancing
     * (InvalidStateStoreException is a StreamsException, not an IllegalStateException). Retry shortly.
     */
    @ExceptionHandler(InvalidStateStoreException.class)
    public ProblemDetail handleStoreNotReady(InvalidStateStoreException ex) {
        log.warn("State store not queryable yet: {}", ex.getMessage());
        var problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "State stores are not ready (Kafka Streams is starting or rebalancing). Retry shortly.");
        problem.setProperty("retryAfterSeconds", 5);
        return problem;
    }

    /** Handles service unavailable. */
    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail handleServiceUnavailable(IllegalStateException ex) {
        log.error("Service unavailable: {}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, ex.getMessage());
    }
}
