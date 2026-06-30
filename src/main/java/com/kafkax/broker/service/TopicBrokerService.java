package com.kafkax.broker.service;

import com.kafkax.broker.model.Message;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class TopicBrokerService {

    private final Map<String, Queue<Message>> topics = new ConcurrentHashMap<>();
    Map<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();

    public void publish(String topic, String payload) {
        topics.computeIfAbsent(topic, t -> new ConcurrentLinkedQueue<>());
        topicOffsets.computeIfAbsent(topic, t -> new AtomicLong(0));
        long offset = topicOffsets.get(topic).getAndIncrement();
        Message message = new Message(offset, payload, Instant.now());
        topics.get(topic).offer(message);
    }

    public Message consume(String topic) {
        Queue<Message> queue = topics.get(topic);
        if (queue == null) {
            return null;
        }
        return queue.poll();
    }

    public int topicSize(String topic) {
        Queue<Message> queue = topics.get(topic);
        return queue == null ? 0 : queue.size();
    }
}
