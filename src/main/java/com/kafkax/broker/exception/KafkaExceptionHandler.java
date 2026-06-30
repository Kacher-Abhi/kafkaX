package com.kafkax.broker.exception;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class KafkaExceptionHandler {

    @ExceptionHandler(TopicNotFoundException.class)
    public ResponseEntity<String> handleNotFound(TopicNotFoundException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }

    @ExceptionHandler(TopicAlreadyExistsException.class)
    public ResponseEntity<String> handleExists(TopicAlreadyExistsException ex) {
        return ResponseEntity.badRequest().body(ex.getMessage());
    }
}
