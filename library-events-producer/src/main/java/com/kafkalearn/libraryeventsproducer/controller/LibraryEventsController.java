package com.kafkalearn.libraryeventsproducer.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalearn.libraryeventsproducer.domain.LibraryEvent;
import com.kafkalearn.libraryeventsproducer.producer.LibraryEventsProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {

    private final LibraryEventsProducer libraryEventsProducer;

    public LibraryEventsController(LibraryEventsProducer libraryEventsProducer) {
        this.libraryEventsProducer = libraryEventsProducer;
    }

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createNewBook(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException {

        log.info("libraryEvent : {} ",libraryEvent);
        libraryEventsProducer.sendLibraryEvent(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
