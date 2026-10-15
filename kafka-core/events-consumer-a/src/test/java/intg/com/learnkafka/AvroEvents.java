package com.learnkafka;

import com.learnkafka.avro.BookAvro;
import com.learnkafka.event.EventMetadata;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.avro.LibraryEventTypeAvro;

import com.learnavro.domain.generated.Address;
import com.learnavro.domain.generated.CoffeeOrder;
import com.learnavro.domain.generated.OrderLineItem;
import com.learnavro.domain.generated.PickUp;
import com.learnavro.domain.generated.Size;
import com.learnavro.domain.generated.Store;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Test helpers: build Avro library events from readable JSON, and decode Avro bytes. */
public final class AvroEvents {

    /**
     * Confluent's in-memory Schema Registry. Every serializer/deserializer in the JVM that uses
     * the same mock:// scope shares it — no Schema Registry container needed in tests.
     */
    public static final String REGISTRY = "mock://events-consumer-test";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private AvroEvents() {
    }

    /** {"libraryEventId":..,"libraryEventType":"NEW","book":{..}} → Avro LibraryEventAvro. */
    public static LibraryEventAvro fromJson(String json) {
        JsonNode node = MAPPER.readTree(json);
        JsonNode book = node.get("book");
        JsonNode id = node.get("libraryEventId");
        return LibraryEventAvro.newBuilder()
                .setLibraryEventId(id == null || id.isNull() ? null : id.asInt())
                .setLibraryEventType(LibraryEventTypeAvro.valueOf(node.get("libraryEventType").asString()))
                .setBook(BookAvro.newBuilder()
                        .setBookId(book.get("bookId").asInt())
                        .setBookName(book.get("bookName").asString())
                        .setBookAuthor(book.get("bookAuthor").asString())
                        .build())
                .build();
    }

    /** Decodes Confluent-framed Avro bytes (magic byte + schema id + payload) from the DLT. */
    public static Object decode(String topic, byte[] bytes) {
        try (var deserializer = new KafkaAvroDeserializer()) {
            deserializer.configure(Map.of(
                    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, REGISTRY,
                    KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true), false);
            return deserializer.deserialize(topic, bytes);
        }
    }

    /** A minimal valid CoffeeOrder (same shape the producer's /v1/coffee-orders builds). */
    public static CoffeeOrder coffeeOrder(String name) {
        var store = Store.newBuilder().setId(1).setAddress(Address.newBuilder()
                .setAddressLine1("123 Coffee Lane").setCity("Seattle").setStateProvince("WA")
                .setCountry("USA").setZip("98101").build()).build();
        var latte = OrderLineItem.newBuilder().setName("Latte").setSize(Size.MEDIUM)
                .setQuantity(1).setCost(new BigDecimal("4.50")).build();
        return CoffeeOrder.newBuilder().setId(UUID.randomUUID()).setName(name).setNickName("")
                .setStore(store).setOrderLineItems(List.of(latte)).setOrderedTime(Instant.now())
                .setPickUp(PickUp.IN_STORE).setStatus("NEW").build();
    }

    /** A copy of the event carrying base-event metadata (as the producer stamps it). */
    public static LibraryEventAvro withMetadata(LibraryEventAvro event, EventMetadata metadata) {
        return LibraryEventAvro.newBuilder(event).setMetadata(metadata.toAvro()).build();
    }
}
