package com.kafkalearn.libraryeventsproducer.controller;

import com.kafkalearn.libraryeventsproducer.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventsController {
    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> createNewBook(@RequestBody LibraryEvent libraryEvent) {

        log.info("libraryEvent : {} ",libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
