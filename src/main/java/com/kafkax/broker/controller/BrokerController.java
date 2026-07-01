package com.kafkax.broker.controller;

import com.kafkax.broker.dto.MessageRequest;
import com.kafkax.broker.dto.TopicRequest;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;


import com.kafkax.broker.dto.MessageResponse;
import com.kafkax.broker.dto.PublishRequest;
import com.kafkax.broker.model.Message;
import com.kafkax.broker.service.TopicBrokerService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/broker")
@AllArgsConstructor
public class BrokerController {

    private final TopicBrokerService brokerService;

    @PostMapping("/topics/{topic}/publish")
    public ResponseEntity<String> publish(@PathVariable String topic, @RequestBody MessageRequest request) throws IOException {
        brokerService.publish(topic, request.message());
        return ResponseEntity.ok("Published message : " + request.message());
    }

    @GetMapping("/topics/{topic}/consume")
    public ResponseEntity<MessageResponse> consume(@PathVariable String topic, @RequestParam String consumeId, @RequestParam int partition) {
        Message message = brokerService.consume(topic, partition,consumeId);
        if (message == null) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(new MessageResponse(message.offset(), message.payload()));
    }

    @GetMapping("/size/{topic}")
    public Integer size(@PathVariable String topic) {
        return brokerService.topicSize(topic);
    }

    @PostMapping("/topics")
    public ResponseEntity<String> createTopic(@RequestBody TopicRequest request) throws IOException {
        brokerService.createTopic(request.name(), request.partitions());
        return ResponseEntity.ok("Topic created");
    }

    @GetMapping("/topics")
    public ResponseEntity<Set<String>> topics() {
        return ResponseEntity.ok(brokerService.listTopics());
    }

    @DeleteMapping("/topics/{topic}")
    public ResponseEntity<String> deleteTopic(@PathVariable String topic) {
        brokerService.deleteTopic(topic);
        return ResponseEntity.ok("Topic deleted");
    }

//    @PostMapping("/test/concurrent")
//    public String concurrentTest() {
//        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
//            for (int i = 0; i < 1000; i++) {
//                int finalI = i;
//                executor.submit(() -> {
//                    try {
//                        brokerService.publish("orders", "message-" + finalI);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//            }
//            executor.shutdown();
//        }
//        return "Submitted";
//    }
}
