package com.kafkax.broker.service;

import com.kafkax.broker.model.Message;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.kafkax.broker.utils.Constants.*;

@Service
public class FileStorageService {

    private final Object writeLock = new Object();

    public void createTopic(String topic, int partitionCount) throws IOException {
        Path topicPath = Paths.get(DATA_DIR, "topics", topic);
        Files.createDirectories(topicPath);
        for (int i = 0; i < partitionCount; i++) {
            Path partitionFile = topicPath.resolve(PARTITION_PREFIX + i + ".log");
            if (!Files.exists(partitionFile)) {
                Files.createFile(partitionFile);
            }
        }
    }

    public void appendMessage(String topic, int partition, Message message) throws IOException {
        synchronized (writeLock) {
            Path file = Paths.get(DATA_DIR, "topics", topic, PARTITION_PREFIX + partition + ".log");
            String line = message.offset() + "|" + message.timestamp() + "|" + message.payload() + System.lineSeparator();
            Files.writeString(file, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    public List<Message> loadMessages(String topic, int partition) throws IOException {
        Path file = Paths.get(DATA_DIR, "topics", topic, PARTITION_PREFIX + partition + ".log");
        if (!Files.exists(file)) {
            return new ArrayList<>();
        }
        List<String> lines = Files.readAllLines(file);
        List<Message> messages = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split("\\|", 3);
            messages.add(new Message(Long.parseLong(parts[0]), parts[2], Instant.parse(parts[1])));
        }
        return messages;
    }

    public void saveConsumerOffset(String topic, String consumerId, long offset) throws IOException {
        Path dir = Paths.get(DATA_DIR, OFFSET_DIR, topic);
        Files.createDirectories(dir);
        if (!consumerId.contains("consumer")) {
            consumerId = "consumer" + consumerId;
        }
        Path file = dir.resolve(consumerId + "." + OFFSET);
        Files.writeString(file, String.valueOf(offset), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    public long loadConsumerOffset(String topic, String consumerId) throws IOException {
        if (!consumerId.contains("consumer")) {
            consumerId = "consumer" + consumerId;
        }
        Path file = Paths.get(DATA_DIR, OFFSET_DIR, topic, consumerId + "." + OFFSET);
        if (!Files.exists(file)) {
            return 0;
        }
        return Long.parseLong(Files.readString(file).trim());
    }
}
