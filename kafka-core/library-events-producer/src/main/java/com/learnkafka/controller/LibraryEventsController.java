package com.learnkafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
@Tag(name = "Library Events", description = "Publish library events to Kafka")
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/libraryevent")
    @Operation(summary = "Publish a new library event",
            responses = {
                    @ApiResponse(responseCode = "201", description = "Event published"),
                    @ApiResponse(responseCode = "400", description = "Validation error")
            })
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        var event = LibraryEvent.builder()
                .libraryEventId(libraryEvent.libraryEventId())
                .libraryEventType(LibraryEventType.NEW)
                .book(libraryEvent.book())
                .build();
        libraryEventProducer.sendLibraryEventWithHeaders(event);
        log.info("Library event sent : {}", event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    @PutMapping("/libraryevent")
    @Operation(summary = "Update an existing library event",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Event updated"),
                    @ApiResponse(responseCode = "400", description = "Missing libraryEventId or validation error")
            })
    public ResponseEntity<LibraryEvent> putLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.badRequest().build();
        }
        var event = LibraryEvent.builder()
                .libraryEventId(libraryEvent.libraryEventId())
                .libraryEventType(LibraryEventType.UPDATE)
                .book(libraryEvent.book())
                .build();
        libraryEventProducer.sendLibraryEventWithHeaders(event);
        log.info("Library event updated : {}", event);
        return ResponseEntity.ok(event);
    }
}
