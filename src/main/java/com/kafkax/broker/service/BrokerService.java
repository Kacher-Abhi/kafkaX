package com.kafkax.broker.service;

import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
@AllArgsConstructor
public class BrokerService {
    private final Queue<String> queue = new ConcurrentLinkedQueue<>();

    public void publish(String message) {
        queue.add(message);
        System.out.println("Message Published : " + message);
    }

    public String consume() {
        return queue.poll();
    }

    public int size() {
        return queue.size();
    }
}
