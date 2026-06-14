package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.service.OrderStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for querying aggregated order counts and revenue from Kafka Streams state stores.
 *
 * GoF Factory Method: ResponseEntity is produced per endpoint as a factory method result.
 */
@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
@Slf4j
public class OrdersController {

    private final OrderStoreService orderStoreService;

    @GetMapping("/count/{orderType}")
    public ResponseEntity<List<OrderCountPerStoreDTO>> getOrdersCount(@PathVariable String orderType) {
        return ResponseEntity.ok(orderStoreService.getOrderCount(orderType));
    }

    @GetMapping("/count/{orderType}/location/{locationId}")
    public ResponseEntity<OrderCountPerStoreDTO> getOrdersCountByLocation(
            @PathVariable String orderType,
            @PathVariable String locationId) {
        var result = orderStoreService.getOrderCountByLocationId(orderType, locationId);
        return result != null ? ResponseEntity.ok(result) : ResponseEntity.notFound().build();
    }

    @GetMapping("/count/all")
    public ResponseEntity<List<AllOrdersCountPerStoreDTO>> getAllOrdersCount() {
        return ResponseEntity.ok(orderStoreService.getAllOrdersCount());
    }

    @GetMapping("/revenue/{orderType}")
    public ResponseEntity<List<OrderRevenueDTO>> getOrdersRevenue(@PathVariable String orderType) {
        return ResponseEntity.ok(orderStoreService.getOrderRevenue(orderType));
    }

    @GetMapping("/revenue/{orderType}/location/{locationId}")
    public ResponseEntity<OrderRevenueDTO> getOrdersRevenueByLocation(
            @PathVariable String orderType,
            @PathVariable String locationId) {
        var result = orderStoreService.getOrderRevenueByLocationId(orderType, locationId);
        return result != null ? ResponseEntity.ok(result) : ResponseEntity.notFound().build();
    }
}
