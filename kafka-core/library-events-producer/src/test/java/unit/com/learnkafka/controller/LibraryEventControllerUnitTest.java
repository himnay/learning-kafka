package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.Book;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    LibraryEventProducer libraryEventProducer;

    private Book validBook() {
        return Book.builder().bookId(123).bookAuthor("Dilip").bookName("Kafka using Spring Boot").build();
    }

    @Test
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
    void postLibraryEvent_invalidBook_returns400() throws Exception {
        var invalidBook  = Book.builder().bookId(null).bookAuthor(null).bookName("Kafka using Spring Boot").build();
        var event = LibraryEvent.builder().libraryEventId(null).book(invalidBook).build();

        mockMvc.perform(post("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest())
                .andExpect(content().string("book.bookAuthor - must not be blank, book.bookId - must not be null"));
    }

    @Test
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
    void putLibraryEvent_nullId_returns400() throws Exception {
        var event = LibraryEvent.builder().libraryEventId(null).book(validBook()).build();

        mockMvc.perform(put("/v1/libraryevent")
                        .content(objectMapper.writeValueAsString(event))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
}
