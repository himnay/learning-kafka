package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {

    private static final int MAX_ERRORS = 10;
    private final AtomicInteger errorCounter = new AtomicInteger(0);

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
            ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Deserialization exception: {} for record: {}", exception.getMessage(), record, exception);
        int count = errorCounter.incrementAndGet();
        log.warn("Deserialization error count: {}", count);
        return count <= MAX_ERRORS
                ? DeserializationHandlerResponse.CONTINUE
                : DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
