package com.learnkafka.retry.model;

/** What one run did. */
public record DrainResult(int processed, int requeued, int deadLettered) {}
