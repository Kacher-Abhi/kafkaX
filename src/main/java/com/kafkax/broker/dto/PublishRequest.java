package com.kafkax.broker.dto;

public record PublishRequest(
        String topic,
        String message
) {
}
