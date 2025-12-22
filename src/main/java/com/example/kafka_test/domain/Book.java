package com.example.kafka_test.domain;

public record Book(
        Integer bookId,
        String bookAuthor,
        String bookName
) {
}
