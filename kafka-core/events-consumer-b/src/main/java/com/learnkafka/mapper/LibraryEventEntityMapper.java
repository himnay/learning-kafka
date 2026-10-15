package com.learnkafka.mapper;

import com.learnkafka.avro.BookAvro;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.avro.LibraryEventTypeAvro;

import com.learnkafka.entity.Book;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.entity.LibraryEventType;

/**
 * Maps the Avro record read from Kafka (generated from schemas/src/main/avro/LibraryEvent.avsc)
 * to the JPA entities stored in PostgreSQL.
 */
public final class LibraryEventEntityMapper {

    private LibraryEventEntityMapper() {
    }

    /** Avro → JPA entity (with the Book ↔ LibraryEvent back-reference set). */
    public static LibraryEvent toEntity(LibraryEventAvro avro) {
        var avroBook = avro.getBook();
        var libraryEvent = LibraryEvent.builder()
                .libraryEventId(avro.getLibraryEventId())
                .libraryEventType(LibraryEventType.valueOf(avro.getLibraryEventType().name()))
                .build();
        var book = Book.builder()
                .bookId(avroBook.getBookId())
                .bookName(avroBook.getBookName().toString())     // Avro strings are CharSequence (Utf8)
                .bookAuthor(avroBook.getBookAuthor().toString())
                .libraryEvent(libraryEvent)
                .build();
        libraryEvent.setBook(book);
        return libraryEvent;
    }

    /** JPA entity (after save, so the id is set) → Avro, for the compacted state topic. */
    public static LibraryEventAvro toAvro(LibraryEvent entity) {
        var book = entity.getBook();
        return LibraryEventAvro.newBuilder()
                .setLibraryEventId(entity.getLibraryEventId())
                .setLibraryEventType(LibraryEventTypeAvro.valueOf(entity.getLibraryEventType().name()))
                .setBook(BookAvro.newBuilder()
                        .setBookId(book.getBookId())
                        .setBookName(book.getBookName())
                        .setBookAuthor(book.getBookAuthor())
                        .build())
                .build();
    }
}
