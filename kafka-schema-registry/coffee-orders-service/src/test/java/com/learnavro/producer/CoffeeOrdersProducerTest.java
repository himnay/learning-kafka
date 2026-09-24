package com.learnavro.producer;

import com.learnavro.domain.generated.CoffeeOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@DisplayName("CoffeeOrdersProducer REST endpoint")
class CoffeeOrdersProducerTest {

    @Mock
    KafkaTemplate<String, CoffeeOrder> kafkaTemplate;

    @InjectMocks
    CoffeeOrdersProducer controller;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    @DisplayName("POST publishes an Avro CoffeeOrder keyed by id and returns a JSON view (201)")
    void publishesOrderAndReturnsJsonView() throws Exception {
        when(kafkaTemplate.send(anyString(), anyString(), any(CoffeeOrder.class)))
                .thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/v1/coffee-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Grace\",\"nickName\":\"g\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.name").value("Grace"))
                .andExpect(jsonPath("$.status").value("NEW"));

        var key = ArgumentCaptor.forClass(String.class);
        var order = ArgumentCaptor.forClass(CoffeeOrder.class);
        verify(kafkaTemplate).send(eq(CoffeeOrdersProducer.TOPIC), key.capture(), order.capture());
        assertThat(key.getValue()).isEqualTo(order.getValue().getId().toString());
        assertThat(order.getValue().getOrderLineItems()).hasSize(1);
    }

    @Test
    @DisplayName("blank name is rejected with 400 and nothing is published")
    void blankNameRejected() throws Exception {
        mockMvc.perform(post("/v1/coffee-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest());

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any(CoffeeOrder.class));
    }
}
