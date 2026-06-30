package com.kafkax.broker.service;

import com.kafkax.broker.exception.TopicAlreadyExistsException;
import com.kafkax.broker.exception.TopicNotFoundException;
import com.kafkax.broker.model.Message;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class TopicBrokerService {

    private final Map<String, Queue<Message>> topics = new ConcurrentHashMap<>();
    Map<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();

    public void publish(String topic, String payload) {
        Queue<Message> queue = topics.get(topic);
        if (queue == null) {
            throw new TopicNotFoundException(topic);
        }
        long offset = topicOffsets.get(topic).getAndIncrement();
        queue.offer(new Message(offset, payload, Instant.now()));
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

    public void createTopic(String topic) {
        if (topics.containsKey(topic)) {
            throw new TopicAlreadyExistsException(topic);
        }
        topics.put(topic, new ConcurrentLinkedQueue<>());
        topicOffsets.put(topic, new AtomicLong(0));
    }

    public void deleteTopic(String topic) {
        if (!topics.containsKey(topic)) {
            throw new TopicNotFoundException(topic);
        }
        topics.remove(topic);
        topicOffsets.remove(topic);
    }

    public Set<String> listTopics() {
        return topics.keySet();
    }
}
