package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.exceptionhandler.OrdersExceptionHandler;
import com.learnkafkastreams.service.OrderStoreService;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** REST layer of the interactive-query API: routing, parameter binding, and error mapping. */
@ExtendWith(MockitoExtension.class)
class OrdersControllerTest {

    @Mock
    OrderStoreService orderStoreService;

    @InjectMocks
    OrdersController controller;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new OrdersExceptionHandler())
                .build();
    }

    @Test
    @DisplayName("GET /count/{orderType} returns the count per store")
    void countByType() throws Exception {
        when(orderStoreService.getOrderCount("general"))
                .thenReturn(List.of(new OrderCountPerStoreDTO("store_1234", 726L)));

        mockMvc.perform(get("/v1/orders/count/general"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].locationId").value("store_1234"))
                .andExpect(jsonPath("$[0].orderCount").value(726));
    }

    @Test
    @DisplayName("GET /count/{orderType}/location/{id} for an unknown store returns 404")
    void countByLocation_unknownStore_404() throws Exception {
        when(orderStoreService.getOrderCountByLocationId("general", "store_9999")).thenReturn(null);

        mockMvc.perform(get("/v1/orders/count/general/location/store_9999"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("An unknown order type returns 400 with a problem detail")
    void unknownOrderType_400() throws Exception {
        when(orderStoreService.getOrderCount("pizza"))
                .thenThrow(new IllegalArgumentException("Unknown orderType: pizza (expected general or restaurant)"));

        mockMvc.perform(get("/v1/orders/count/pizza"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("Unknown orderType: pizza (expected general or restaurant)"));
    }

    @Test
    @DisplayName("While Kafka Streams is starting or rebalancing the API returns 503, not 500")
    void storeNotReady_503() throws Exception {
        when(orderStoreService.getOrderRevenue("general"))
                .thenThrow(new InvalidStateStoreException("Cannot get state store general_orders_revenue because the stream thread is STARTING"));

        mockMvc.perform(get("/v1/orders/revenue/general"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.retryAfterSeconds").value(5));
    }

    @Test
    @DisplayName("GET /windows/count binds from_time / to_time as ISO date-times")
    void windowsCount_bindsTimeRange() throws Exception {
        var from = LocalDateTime.parse("2026-02-16T11:27:00");
        var to = LocalDateTime.parse("2026-02-16T11:28:00");
        when(orderStoreService.getAllOrderCountWindows(from, to)).thenReturn(List.of(
                new OrdersCountPerStoreByWindowsDTO("store_1234", 27L, OrderType.RESTAURANT, from, from.plusSeconds(30))));

        mockMvc.perform(get("/v1/orders/windows/count")
                        .param("from_time", "2026-02-16T11:27:00")
                        .param("to_time", "2026-02-16T11:28:00"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].orderCount").value(27))
                .andExpect(jsonPath("$[0].orderType").value("RESTAURANT"))
                .andExpect(jsonPath("$[0].startWindow").value("2026-02-16T11:27:00"))
                .andExpect(jsonPath("$[0].endWindow").value("2026-02-16T11:27:30"));
    }

    @Test
    @DisplayName("GET /windows/count/{orderType} without a time range passes nulls (all retained windows)")
    void windowsCountByType_noRange() throws Exception {
        when(orderStoreService.getOrderCountWindows(any(), isNull(), isNull())).thenReturn(List.of());

        mockMvc.perform(get("/v1/orders/windows/count/restaurant"))
                .andExpect(status().isOk());

        verify(orderStoreService).getOrderCountWindows("restaurant", null, null);
    }

    @Test
    @DisplayName("A from_time that is not a date-time returns 400")
    void windowsCount_badTime_400() throws Exception {
        mockMvc.perform(get("/v1/orders/windows/count").param("from_time", "yesterday"))
                .andExpect(status().isBadRequest());
    }
}
