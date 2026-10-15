package com.learnkafka.controller;

import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.event.EventMetadataFactory;
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
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Tag(name = "Library Events", description = "Publish library events to Kafka")
public class LibraryEventsController {

    /** Optional request header; copied into the event's metadata.correlationId. */
    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";

    private final LibraryEventProducer libraryEventProducer;
    private final EventMetadataFactory eventMetadataFactory;

    /**
     * Publishes a NEW library event. The id must be absent: the consumers' database assigns it
     * (a NEW event that carries an id would be merged as an existing row and fail downstream).
     */
    @PostMapping("/libraryevent")
    @Operation(summary = "Publish a new library event",
            responses = {
                    @ApiResponse(responseCode = "201", description = "Event published"),
                    @ApiResponse(responseCode = "400", description = "Validation error, or libraryEventId was sent")
            })
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent,
            @RequestHeader(name = CORRELATION_ID_HEADER, required = false) String correlationId) {
        if (libraryEvent.libraryEventId() != null) {
            return ResponseEntity.badRequest().build();
        }
        var event = LibraryEvent.builder()
                .metadata(eventMetadataFactory.newEvent(LibraryEvent.TYPE_PREFIX + LibraryEventType.NEW, correlationId))
                .libraryEventId(libraryEvent.libraryEventId())
                .libraryEventType(LibraryEventType.NEW)
                .book(libraryEvent.book())
                .build();
        libraryEventProducer.sendLibraryEventWithHeaders(event);
        log.info("Library event sent : {}", event);
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }

    /** Publishes an UPDATE for an existing library event; libraryEventId is required. */
    @PutMapping("/libraryevent")
    @Operation(summary = "Update an existing library event",
            responses = {
                    @ApiResponse(responseCode = "200", description = "Event updated"),
                    @ApiResponse(responseCode = "400", description = "Missing libraryEventId or validation error")
            })
    public ResponseEntity<LibraryEvent> putLibraryEvent(
            @RequestBody @Valid LibraryEvent libraryEvent,
            @RequestHeader(name = CORRELATION_ID_HEADER, required = false) String correlationId) {
        if (libraryEvent.libraryEventId() == null) {
            return ResponseEntity.badRequest().build();
        }
        var event = LibraryEvent.builder()
                .metadata(eventMetadataFactory.newEvent(LibraryEvent.TYPE_PREFIX + LibraryEventType.UPDATE, correlationId))
                .libraryEventId(libraryEvent.libraryEventId())
                .libraryEventType(LibraryEventType.UPDATE)
                .book(libraryEvent.book())
                .build();
        libraryEventProducer.sendLibraryEventWithHeaders(event);
        log.info("Library event updated : {}", event);
        return ResponseEntity.ok(event);
    }
}
