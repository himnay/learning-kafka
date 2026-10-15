package com.learnkafka.coffee;

import com.learnkafka.domain.CoffeeOrderResponse;
import com.learnkafka.event.EventMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@DisplayName("CoffeeOrdersController REST endpoint")
class CoffeeOrdersControllerTest {

    @Mock
    CoffeeOrderService coffeeOrderService;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new CoffeeOrdersController(coffeeOrderService)).build();
    }

    @Test
    @DisplayName("POST returns 201 with the stored order and its event metadata")
    void returns201WithTheServiceResponse() throws Exception {
        var metadata = EventMetadata.newEvent("CoffeeOrder.PLACED", "events-producer", null);
        when(coffeeOrderService.place(any(), any())).thenReturn(
                new CoffeeOrderResponse(UUID.randomUUID().toString(), "Grace", "g", "NEW", Instant.now(), metadata));

        mockMvc.perform(post("/v1/coffee-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"Grace\",\"nickName\":\"g\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("Grace"))
                .andExpect(jsonPath("$.metadata.eventType").value("CoffeeOrder.PLACED"));
    }

    @Test
    @DisplayName("A blank name is rejected with 400 and nothing is stored")
    void blankNameRejected() throws Exception {
        mockMvc.perform(post("/v1/coffee-orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"\"}"))
                .andExpect(status().isBadRequest());

        verify(coffeeOrderService, never()).place(any(), any());
    }
}
