package com.kafkax.broker.controller;

import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import com.kafkax.broker.dto.MessageResponse;
import com.kafkax.broker.dto.PublishRequest;
import com.kafkax.broker.model.Message;
import com.kafkax.broker.service.TopicBrokerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/broker")
@AllArgsConstructor
public class BrokerController {

    private final TopicBrokerService brokerService;

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestBody PublishRequest request) {
        brokerService.publish(request.topic(), request.message());
        return ResponseEntity.ok("Published");
    }

    @GetMapping("/consume/{topic}")
    public ResponseEntity<MessageResponse> consume(@PathVariable String topic) {
        Message message = brokerService.consume(topic);
        if (message == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(new MessageResponse(message.offset(), message.payload()));
    }

    @GetMapping("/size/{topic}")
    public Integer size(@PathVariable String topic) {
        return brokerService.topicSize(topic);
    }
}
