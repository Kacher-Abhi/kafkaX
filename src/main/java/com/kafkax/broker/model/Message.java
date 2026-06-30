package com.kafkax.broker.model;

import java.time.Instant;

public record Message(
        Long offset,
        String payload,
        Instant timestamp
) {
}