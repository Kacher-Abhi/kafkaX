package com.kafkax.broker.exception;

public class TopicAlreadyExistsException extends RuntimeException {
    public TopicAlreadyExistsException(String topic) {
        super("Topic already exists: " + topic);
    }
}
