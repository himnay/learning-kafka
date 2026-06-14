package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

/**
 * Handles uncaught exceptions in Kafka Streams processing threads.
 *
 * Strategy Pattern: response type (REPLACE_THREAD vs SHUTDOWN_APPLICATION) is
 * selected based on the nature of the exception.
 */
@Slf4j
public class StreamsProcessorCustomErrorHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Uncaught exception in Kafka Streams thread: {}", exception.getMessage(), exception);

        if (exception instanceof StreamsException streamsEx) {
            var cause = streamsEx.getCause();
            if (cause != null && "Transient Error".equals(cause.getMessage())) {
                log.warn("Transient error detected — replacing thread");
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
        }

        log.error("Non-transient error — shutting down application");
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
