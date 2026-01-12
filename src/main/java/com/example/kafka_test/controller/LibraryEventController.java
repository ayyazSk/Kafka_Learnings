package com.example.kafka_test.controller;


import com.example.kafka_test.domain.LibraryEvent;
import com.example.kafka_test.producer.LibraryEventProducer;
import org.apache.commons.logging.Log;
import lombok.extern.slf4j.Slf4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;
    private static final Logger log = LogManager.getLogger(LibraryEventController.class);

    @PostMapping("/v1/libraryEvent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(
            @RequestBody LibraryEvent libraryEvent
    )
    {
    log.info("LibraryEvent : {} ", libraryEvent);
    // invoke kafka producer

        libraryEventProducer.publishLibraryEvent(libraryEvent);

        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
