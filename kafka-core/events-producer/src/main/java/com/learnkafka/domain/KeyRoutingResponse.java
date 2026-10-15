package com.learnkafka.domain;

/**
 * Where a key-routing record landed. {@code matchedSubstring} is the rule that routed it, or null
 * when no substring matched and the whole key was hashed.
 */
public record KeyRoutingResponse(
        String key,
        String matchedSubstring,
        int partition,
        long offset
) {}
