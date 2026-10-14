package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import com.learnkafkastreams.domain.OrderRevenueDTO;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import com.learnkafkastreams.service.OrderStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

/**
 * REST API for querying aggregated order counts and revenue from Kafka Streams state stores.
 *
 * GoF Factory Method: ResponseEntity is produced per endpoint as a factory method result.
 */
@Slf4j
@RestController
@RequiredArgsConstructor
@RequestMapping("/v1/orders")
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

    /** Revenue per store joined with the store's details (KTable-KTable join with the stores topic). */
    @GetMapping("/revenue/{orderType}/with-address")
    public ResponseEntity<List<TotalRevenueWithAddress>> getOrdersRevenueWithAddress(@PathVariable String orderType) {
        return ResponseEntity.ok(orderStoreService.getOrderRevenueWithAddress(orderType));
    }

    // ── Windowed (30 s tumbling windows, event time) ─────────────────────────
    // Optional from_time / to_time (ISO local date-time, UTC) select windows whose START is in [from, to].

    @GetMapping("/windows/count/{orderType}")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getOrdersCountWindows(
            @PathVariable String orderType,
            @RequestParam(name = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(name = "to_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return ResponseEntity.ok(orderStoreService.getOrderCountWindows(orderType, from, to));
    }

    @GetMapping("/windows/count")
    public ResponseEntity<List<OrdersCountPerStoreByWindowsDTO>> getAllOrdersCountWindows(
            @RequestParam(name = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(name = "to_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return ResponseEntity.ok(orderStoreService.getAllOrderCountWindows(from, to));
    }

    @GetMapping("/windows/revenue/{orderType}")
    public ResponseEntity<List<OrdersRevenuePerStoreByWindowsDTO>> getOrdersRevenueWindows(
            @PathVariable String orderType,
            @RequestParam(name = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(name = "to_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {
        return ResponseEntity.ok(orderStoreService.getOrderRevenueWindows(orderType, from, to));
    }
}
