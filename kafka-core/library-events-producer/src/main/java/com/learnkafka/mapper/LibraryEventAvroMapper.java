package com.learnkafka.mapper;

import com.learnkafka.avro.BookAvro;
import com.learnkafka.avro.LibraryEventAvro;
import com.learnkafka.avro.LibraryEventTypeAvro;

import com.learnkafka.domain.LibraryEvent;

/**
 * Maps the validated REST model (domain records) to the Avro classes generated from
 * schemas/src/main/avro/LibraryEvent.avsc — the type that actually goes on the wire.
 */
public final class LibraryEventAvroMapper {

    private LibraryEventAvroMapper() {
    }

    /** Domain → Avro. Builders validate required fields against the schema. */
    public static LibraryEventAvro toAvro(LibraryEvent libraryEvent) {
        var book = libraryEvent.book();
        return LibraryEventAvro.newBuilder()
                .setLibraryEventId(libraryEvent.libraryEventId())
                .setLibraryEventType(LibraryEventTypeAvro.valueOf(libraryEvent.libraryEventType().name()))
                .setBook(BookAvro.newBuilder()
                        .setBookId(book.bookId())
                        .setBookName(book.bookName())
                        .setBookAuthor(book.bookAuthor())
                        .build())
                .build();
    }
}
