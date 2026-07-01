package com.kafkax.broker.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
public class Topic {
    private final String name;
    private final int partitionCount;
}
