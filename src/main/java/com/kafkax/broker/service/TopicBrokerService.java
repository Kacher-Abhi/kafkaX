package com.kafkax.broker.service;

import com.kafkax.broker.exception.TopicAlreadyExistsException;
import com.kafkax.broker.exception.TopicNotFoundException;
import com.kafkax.broker.model.Message;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.kafkax.broker.utils.Constants.DATA_DIR;

@Service
@AllArgsConstructor
public class TopicBrokerService {

    private final FileStorageService fileStorageService;

    private final Map<String, List<Message>> topics = new ConcurrentHashMap<>();
    Map<String, AtomicLong> topicOffsets;


    @PostConstruct
    public void initialize() {
        try {
            Path dataDir = Paths.get(DATA_DIR);
            if (!Files.exists(dataDir)) {
                return;
            }
            Files.list(dataDir).filter(Files::isDirectory).forEach(topicDir -> {
                try {
                    String topic = topicDir.getFileName().toString();
                    List<Message> messages = fileStorageService.loadMessages(topic);
                    topics.put(topic, Collections.synchronizedList(messages));
                    long nextOffset = messages.isEmpty() ? 0 : messages.getLast().offset() + 1;
                    topicOffsets.put(topic, new AtomicLong(nextOffset));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public void publish(String topic, String payload) throws IOException {
        List<Message> messages = topics.get(topic);
        if (messages == null) {
            throw new TopicNotFoundException(topic);
        }
        long offset = topicOffsets.get(topic).getAndIncrement();
        Message message = new Message(offset, payload, Instant.now());
        messages.add(message);
        fileStorageService.appendMessage(topic, message);
    }

    public Message consume(String topic, long offset) {
        List<Message> messages = topics.get(topic);
        if (messages == null) {
            return null;
        }
        if (offset >= messages.size()) {
            return null;
        }
        return messages.get((int) offset);
    }

    public int topicSize(String topic) {
        List<Message> queue = topics.get(topic);
        return queue == null ? 0 : queue.size();
    }

    public void createTopic(String topic) throws IOException {
        if (topics.containsKey(topic)) {
            throw new TopicAlreadyExistsException(topic);
        }
        fileStorageService.createTopic(topic);
        topics.put(topic, Collections.synchronizedList(new ArrayList<>()));
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
