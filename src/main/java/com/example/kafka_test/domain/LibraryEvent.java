package com.example.kafka_test.domain;

import com.example.kafka_test.enums.libraryEventType;

public record LibraryEvent(
        Integer libraryEventId,
        libraryEventType libraryEventType,
        Book book
) {
}
