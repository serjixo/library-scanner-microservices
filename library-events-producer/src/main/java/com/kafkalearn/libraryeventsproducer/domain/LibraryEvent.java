package com.kafkalearn.libraryeventsproducer.domain;


import java.awt.print.Book;

public record LibraryEvent(Integer libraryEventId, LibraryEventType libraryEventType, Book book) {
}
