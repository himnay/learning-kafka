package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

/**
 * Log-and-skip for records that Kafka Streams cannot write to an output or changelog topic:
 * both serialization failures and send failures are logged and the record is dropped
 * ({@code RESUME}), so one bad record does not stop the stream thread.
 *
 * <p>Registered as {@code production.exception.handler} in {@code OrdersStreamsConfiguration}.
 */
@Slf4j
public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {

    @Override
    public Response handleError(ErrorHandlerContext context,
                                ProducerRecord<byte[], byte[]> record,
                                Exception exception) {
        log.error("Failed to send record to topic {} (task {}), skipping it: {}",
                record.topic(), context.taskId(), exception.getMessage(), exception);
        return Response.resume();
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Response handleSerializationError(ErrorHandlerContext context,
                                             ProducerRecord record,
                                             Exception exception,
                                             SerializationExceptionOrigin origin) {
        log.error("Failed to serialize the record {} for topic {} (task {}), skipping it: {}",
                origin, record.topic(), context.taskId(), exception.getMessage(), exception);
        return Response.resume();
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
