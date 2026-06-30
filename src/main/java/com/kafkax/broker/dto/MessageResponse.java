package com.kafkax.broker.dto;

public record MessageResponse(
        Long offset,
        String message
) {
}
