package com.learnkafka.controller;

import tools.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.event.EventMetadataFactory;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class LibraryEventControllerUnitTest {

    @Mock
    LibraryEventProducer libraryEventProducer;

    @Spy
    EventMetadataFactory eventMetadataFactory = new EventMetadataFactory("events-producer");

    @InjectMocks
    LibraryEventsController controller;

    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setControllerAdvice(new LibraryEventControllerAdvice())
                .build();
    }

    private Book validBook() {
        return Book.builder().bookId(123).bookAuthor("Dilip").bookName("Kafka using Spring Boot").build();
    }

    @Test
    @DisplayName("POSTing a valid library event returns HTTP 201 Created")
    void postLibraryEvent_returns201() throws Exception {
        var event = LibraryEvent.builder().libraryEventId(null).book(validBook()).build();
        when(libraryEventProducer.sendLibraryEventWithHeaders(isA(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        mockMvc.perform(post("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());
    }

    @Test
    @DisplayName("POST stamps the base-event metadata (eventId, type, source, correlation id) and returns it")
    void postLibraryEvent_stampsMetadata() throws Exception {
        var event = LibraryEvent.builder().book(validBook()).build();
        when(libraryEventProducer.sendLibraryEventWithHeaders(isA(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        mockMvc.perform(post("/v1/libraryevent")
                        .header("X-Correlation-Id", "req-42")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.metadata.eventId").isNotEmpty())
                .andExpect(jsonPath("$.metadata.eventType").value("LibraryEvent.NEW"))
                .andExpect(jsonPath("$.metadata.eventVersion").value(1))
                .andExpect(jsonPath("$.metadata.source").value("events-producer"))
                .andExpect(jsonPath("$.metadata.correlationId").value("req-42"));

        verify(libraryEventProducer).sendLibraryEventWithHeaders(argThat(e ->
                e.metadata() != null && "LibraryEvent.NEW".equals(e.eventType()) && e.eventId() != null));
    }

    @Test
    @DisplayName("POSTing a library event with an invalid book returns HTTP 400 with field validation error messages")
    void postLibraryEvent_invalidBook_returns400() throws Exception {
        var invalidBook = Book.builder().bookId(null).bookAuthor(null).bookName("Kafka using Spring Boot").build();
        var event = LibraryEvent.builder().libraryEventId(null).book(invalidBook).build();

        mockMvc.perform(post("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("book.bookAuthor - must not be blank, book.bookId - must not be null"));
    }

    @Test
    @DisplayName("POSTing a NEW library event that carries an id returns HTTP 400 and publishes nothing")
    void postLibraryEvent_withId_returns400() throws Exception {
        var event = LibraryEvent.builder().libraryEventId(42).book(validBook()).build();

        mockMvc.perform(post("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());

        verifyNoInteractions(libraryEventProducer);
    }

    @Test
    @DisplayName("PUTting a valid library event update returns HTTP 200 OK")
    void putLibraryEvent_returns200() throws Exception {
        var event = LibraryEvent.builder().libraryEventId(123).book(validBook()).build();
        when(libraryEventProducer.sendLibraryEventWithHeaders(isA(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        mockMvc.perform(put("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("PUTting a library event with a null library event id returns HTTP 400 Bad Request")
    void putLibraryEvent_nullId_returns400() throws Exception {
        var event = LibraryEvent.builder().libraryEventId(null).book(validBook()).build();

        mockMvc.perform(put("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
}
