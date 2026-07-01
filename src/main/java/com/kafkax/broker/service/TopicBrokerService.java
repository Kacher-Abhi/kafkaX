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
import static com.kafkax.broker.utils.Constants.PARTITION_PREFIX;

@Service
@AllArgsConstructor
public class TopicBrokerService {

    private final FileStorageService fileStorageService;

    private final Map<String, Map<Integer, List<Message>>> topics = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> topicOffsets = new ConcurrentHashMap<>();
    private final Object topicLock = new Object();
    private final Map<String, Map<String, AtomicLong>> consumerOffsets = new ConcurrentHashMap<>();
    private final Map<String, Integer> topicPartitionCount = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> partitionSelector = new ConcurrentHashMap<>();

    @PostConstruct
    public void initialize() {
        try {
            Path dataDir = Paths.get(DATA_DIR, "topics");
            if (!Files.exists(dataDir)) {
                return;
            }
            Files.list(dataDir).filter(Files::isDirectory).forEach(topicDir -> {
                try {
                    String topic = topicDir.getFileName().toString();
                    Map<Integer, List<Message>> partitions = new ConcurrentHashMap<>();
                    long highestOffset = -1;
                    List<Path> partitionFiles = Files.list(topicDir)
                            .filter(Files::isRegularFile)
                            .filter(file -> file.getFileName().toString().startsWith(PARTITION_PREFIX)).toList();
                    for (Path partitionFile : partitionFiles) {
                        String fileName = partitionFile.getFileName().toString();
                        int partition = Integer.parseInt(fileName
                                .replace(PARTITION_PREFIX, "").replace(".log", ""));
                        List<Message> messages = fileStorageService.loadMessages(topic, partition);
                        partitions.put(partition, Collections.synchronizedList(messages));
                        if (!messages.isEmpty()) {
                            highestOffset = Math.max(highestOffset, messages.get(messages.size() - 1).offset());
                        }
                    }
                    topics.put(topic, partitions);
                    topicPartitionCount.put(topic, partitions.size());
                    topicOffsets.put(topic, new AtomicLong(highestOffset + 1));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public void publish(String topic, String payload) throws IOException {
        Map<Integer, List<Message>> partitions = topics.get(topic);
        if (partitions == null) {
            throw new TopicNotFoundException(topic);
        }
        synchronized (topicLock) {
            long offset = topicOffsets.get(topic).getAndIncrement();
            int partitionCount = topicPartitionCount.get(topic);
            long next = partitionSelector.computeIfAbsent(topic, t -> new AtomicLong(0)).getAndIncrement();
            int partition = (int) (next % partitionCount);
            Message message = new Message(offset, payload, Instant.now());
            partitions.get(partition).add(message);
            fileStorageService.appendMessage(topic, partition, message);
        }
    }

    public Message consume(String topic, int partition, String consumerId) {
        Map<Integer, List<Message>> topicPartitions = topics.get(topic);
        if (topicPartitions == null) {
            return null;
        }
        List<Message> messages = topicPartitions.get(partition);
        if (messages == null || messages.isEmpty()) {
            return null;
        }
        AtomicLong consumerOffset = getConsumerOffset(topic + "-" + partition, consumerId);
        long offset = consumerOffset.get();
        if (offset >= messages.size()) {
            return null;
        }
        Message message = messages.get((int) offset);
        long nextOffset = consumerOffset.incrementAndGet();
        try {
            fileStorageService.saveConsumerOffset(topic + "-" + partition, consumerId, nextOffset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return message;
    }

    public int topicSize(String topic) {
        Map<Integer, List<Message>> partitions = topics.get(topic);
        if (partitions == null) {
            return 0;
        }
        return partitions.values().stream().mapToInt(List::size).sum();
    }

    public void createTopic(String topic, int partitionCount) throws IOException {
        if (topics.containsKey(topic)) {
            throw new TopicAlreadyExistsException(topic);
        }
        Map<Integer, List<Message>> partitions = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            partitions.put(i, Collections.synchronizedList(new ArrayList<>()));
        }
        topics.put(topic, partitions);
        topicPartitionCount.put(topic, partitionCount);
        fileStorageService.createTopic(topic, partitionCount);
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

    private AtomicLong getConsumerOffset(String topic, String consumerId) {
        consumerOffsets.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
        return consumerOffsets.get(topic).computeIfAbsent(consumerId, c -> {
            try {
                long offset = fileStorageService.loadConsumerOffset(topic, consumerId);
                return new AtomicLong(offset);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }
}
