package com.learnkafka.coffee;

import com.learnavro.domain.generated.CoffeeOrder;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Serializes a CoffeeOrder exactly as the KafkaTemplate used to: Confluent wire format (magic byte +
 * schema id + Avro binary), schema registered or looked up under subject "coffee-orders-value". The bytes
 * go into outbox_event.payload, and Debezium's BinaryDataConverter writes them to Kafka unchanged, so
 * consumers read them as before.
 *
 * Schema Registry is still needed when an order is written (first use of the schema; cached after).
 */
@Component
public class CoffeeOrderPayloadSerializer implements DisposableBean {

    private final KafkaAvroSerializer serializer = new KafkaAvroSerializer();

    public CoffeeOrderPayloadSerializer(@Value("${spring.kafka.producer.properties.schema.registry.url}") String registryUrl) {
        serializer.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl), false);
    }

    public byte[] serialize(CoffeeOrder order) {
        return serializer.serialize(CoffeeOrderService.TOPIC, order);
    }

    @Override
    public void destroy() {
        serializer.close();
    }
}
