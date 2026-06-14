package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerUtil {

    private static final String BOOTSTRAP_SERVERS = System.getProperty(
            "kafka.bootstrap-servers", "localhost:9092");

    private static final KafkaProducer<String, String> PRODUCER = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
    ));

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(PRODUCER::close, "kafka-producer-shutdown"));
    }

    public static RecordMetadata publishMessageSync(String topicName, String key, String message) {
        var producerRecord = new ProducerRecord<>(topicName, key, message);
        try {
            log.info("Publishing: {}", producerRecord);
            return PRODUCER.send(producerRecord).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted publishing to {}: {}", topicName, e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("Execution error publishing to {}: {}", topicName, e.getMessage(), e);
        }
        return null;
    }

    private ProducerUtil() {
    }
}
