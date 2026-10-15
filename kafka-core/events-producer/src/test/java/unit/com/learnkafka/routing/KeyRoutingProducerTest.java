package com.learnkafka.routing;

import com.learnkafka.partitioner.KeySubstringPartitioner;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@DisplayName("KeyRoutingProducer REST endpoint")
class KeyRoutingProducerTest {

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        var controller = new KeyRoutingProducer(kafkaTemplate, KeySubstringPartitioner.parseRules("VIP=0,UK"));   // package-private test constructor
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    @DisplayName("POST returns the partition and offset Kafka acked, and the substring that routed the key (201)")
    void returnsWhereTheRecordLanded() throws Exception {
        when(kafkaTemplate.send(KeyRoutingProducer.TOPIC, "order-VIP-7", "hello"))
                .thenReturn(CompletableFuture.completedFuture(sendResult("order-VIP-7", "hello", 0, 42)));

        mockMvc.perform(post("/v1/key-routing")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"order-VIP-7\",\"message\":\"hello\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.key").value("order-VIP-7"))
                .andExpect(jsonPath("$.matchedSubstring").value("VIP"))
                .andExpect(jsonPath("$.partition").value(0))
                .andExpect(jsonPath("$.offset").value(42));
    }

    @Test
    @DisplayName("A key that contains no configured substring reports matchedSubstring null")
    void unmatchedKey() throws Exception {
        when(kafkaTemplate.send(KeyRoutingProducer.TOPIC, "order-12", "hi"))
                .thenReturn(CompletableFuture.completedFuture(sendResult("order-12", "hi", 2, 7)));

        mockMvc.perform(post("/v1/key-routing")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"order-12\",\"message\":\"hi\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.matchedSubstring").value(nullValue()))
                .andExpect(jsonPath("$.partition").value(2));
    }

    @Test
    @DisplayName("blank key is rejected with 400 and nothing is published")
    void blankKeyRejected() throws Exception {
        mockMvc.perform(post("/v1/key-routing")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\" \",\"message\":\"hi\"}"))
                .andExpect(status().isBadRequest());

        verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("POST /partitions/{partition} sends to that partition and reports no matched substring (201)")
    void explicitPartition() throws Exception {
        when(kafkaTemplate.partitionsFor(KeyRoutingProducer.TOPIC)).thenReturn(threePartitions());
        when(kafkaTemplate.send(KeyRoutingProducer.TOPIC, 2, "order-VIP-7", "hello"))
                .thenReturn(CompletableFuture.completedFuture(sendResult("order-VIP-7", "hello", 2, 5)));

        mockMvc.perform(post("/v1/key-routing/partitions/2")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"order-VIP-7\",\"message\":\"hello\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.key").value("order-VIP-7"))
                .andExpect(jsonPath("$.matchedSubstring").value(nullValue()))
                .andExpect(jsonPath("$.partition").value(2))
                .andExpect(jsonPath("$.offset").value(5));
    }

    @ParameterizedTest(name = "partition {0}")
    @ValueSource(ints = {3, -1})
    @DisplayName("A partition the topic doesn't have is rejected with 400 and nothing is published")
    void partitionOutOfRange(int partition) throws Exception {
        lenient().when(kafkaTemplate.partitionsFor(KeyRoutingProducer.TOPIC)).thenReturn(threePartitions());

        mockMvc.perform(post("/v1/key-routing/partitions/" + partition)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"order-1\",\"message\":\"hi\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("Partition " + partition + " does not exist: topic key-routing has partitions 0-2"));

        verify(kafkaTemplate, never()).send(anyString(), anyInt(), anyString(), anyString());
    }

    private static List<PartitionInfo> threePartitions() {
        var leader = new Node(1, "localhost", 9092);
        var replicas = new Node[]{leader};
        return List.of(
                new PartitionInfo(KeyRoutingProducer.TOPIC, 0, leader, replicas, replicas),
                new PartitionInfo(KeyRoutingProducer.TOPIC, 1, leader, replicas, replicas),
                new PartitionInfo(KeyRoutingProducer.TOPIC, 2, leader, replicas, replicas));
    }

    private static SendResult<String, String> sendResult(String key, String value, int partition, long offset) {
        return new SendResult<>(new ProducerRecord<>(KeyRoutingProducer.TOPIC, key, value),
                new RecordMetadata(new TopicPartition(KeyRoutingProducer.TOPIC, partition), offset, 0, 0L, 0, 0));
    }
}
