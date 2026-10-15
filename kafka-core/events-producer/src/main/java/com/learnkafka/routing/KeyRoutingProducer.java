package com.learnkafka.routing;

import com.learnkafka.domain.KeyRoutingRequest;
import com.learnkafka.domain.KeyRoutingResponse;
import com.learnkafka.partitioner.KeySubstringPartitioner;
import com.learnkafka.partitioner.KeySubstringPartitioner.Rule;
import jakarta.annotation.PreDestroy;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * REST endpoint that publishes plain-text messages to key-routing with a key chosen by the caller.
 * Its template uses KeySubstringPartitioner, so every key containing a configured substring
 * (app.key-routing.substring-partitions) lands on the same partition. The response says which one.
 * POST /partitions/{partition} skips the partitioner and sends to the partition the caller names.
 *
 * It builds its own String/String KafkaTemplate from Boot's producer
 * factory: a second KafkaTemplate bean would make Boot's auto-configured one back off. Everything
 * else (acks, idempotence, zstd compression, linger.ms) is inherited from that factory.
 */
@Slf4j
@RestController
@RequestMapping("/v1/key-routing")
public class KeyRoutingProducer {

    public static final String TOPIC = "key-routing";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final List<Rule> rules;

    @Autowired
    public KeyRoutingProducer(ProducerFactory<?, ?> producerFactory,
                              @Value("${app.key-routing.substring-partitions:}") String rules,
                              @Value("${spring.application.name}") String applicationName) {
        // Parsed here too, so a malformed rule fails at startup instead of on the first send
        this(routingTemplate(producerFactory, rules, applicationName + "-key-routing"),
                KeySubstringPartitioner.parseRules(rules));
    }

    /** For tests: inject the template directly. */
    KeyRoutingProducer(KafkaTemplate<String, String> kafkaTemplate, List<Rule> rules) {
        this.kafkaTemplate = kafkaTemplate;
        this.rules = rules;
    }

    /**
     * Boot's producer settings, plus: String keys and values, and the substring partitioner with
     * its rules as a producer property (it reads them in configure()). Own client id, as for coffee.
     */
    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> routingTemplate(ProducerFactory<?, ?> producerFactory,
                                                                 String rules, String clientId) {
        return new KafkaTemplate<>((ProducerFactory<String, String>) producerFactory, false, Map.of(
                ProducerConfig.CLIENT_ID_CONFIG, clientId,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.PARTITIONER_CLASS_CONFIG, KeySubstringPartitioner.class,
                KeySubstringPartitioner.RULES_CONFIG, rules));
    }

    /** The template is not a Spring bean, so close its producers when the app stops. */
    @PreDestroy
    void close() {
        kafkaTemplate.destroy();
    }

    /** Publishes the message and waits for the broker's ack, so the response can say where it landed. */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public KeyRoutingResponse publish(@RequestBody @Valid KeyRoutingRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        var metadata = kafkaTemplate.send(TOPIC, request.key(), request.message())
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata();
        var substring = KeySubstringPartitioner.matchingRule(rules, request.key()).map(Rule::substring).orElse(null);
        log.info("Routed key={} (substring={}) to partition={} offset={}",
                request.key(), substring, metadata.partition(), metadata.offset());
        return new KeyRoutingResponse(request.key(), substring, metadata.partition(), metadata.offset());
    }

    /**
     * Publishes the message to the partition the caller names. A record that carries a partition
     * never reaches the partitioner, so the key only travels with the record: KeySubstringPartitioner
     * is skipped and matchedSubstring is null. A partition the topic doesn't have is a 400 here;
     * send() itself would block for max.block.ms (60 s) waiting for it to appear, then time out.
     */
    @PostMapping("/partitions/{partition}")
    public ResponseEntity<?> publishToPartition(@PathVariable int partition, @RequestBody @Valid KeyRoutingRequest request)
            throws ExecutionException, InterruptedException, TimeoutException {
        int partitions = kafkaTemplate.partitionsFor(TOPIC).size();
        if (partition < 0 || partition >= partitions) {
            var error = "Partition %d does not exist: topic %s has partitions 0-%d".formatted(partition, TOPIC, partitions - 1);
            log.warn(error);
            return ResponseEntity.badRequest().body(error);
        }
        var metadata = kafkaTemplate.send(TOPIC, partition, request.key(), request.message())
                .get(10, TimeUnit.SECONDS)
                .getRecordMetadata();
        log.info("Sent key={} to requested partition={} offset={}", request.key(), metadata.partition(), metadata.offset());
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(new KeyRoutingResponse(request.key(), null, metadata.partition(), metadata.offset()));
    }
}
