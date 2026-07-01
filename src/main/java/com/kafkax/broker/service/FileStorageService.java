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

import static com.kafkax.broker.utils.Constants.DATA_DIR;
import static com.kafkax.broker.utils.Constants.LOG_FILE;

@Service
public class FileStorageService {

    private final Object writeLock = new Object();

    public void createTopic(String topic) throws IOException {
        Path topicPath = Paths.get(DATA_DIR, topic);
        Files.createDirectories(topicPath);
        Path logFile = topicPath.resolve(LOG_FILE);
        System.out.println(Paths.get(DATA_DIR).toAbsolutePath());
        if (!Files.exists(logFile)) {
            Files.createFile(logFile);
        }
    }

    public void appendMessage(String topic, Message message) throws IOException {
        synchronized (writeLock) {
            Path file = Paths.get(DATA_DIR, topic, LOG_FILE);
            String line = message.offset() + "|" + message.timestamp() + "|" + message.payload() + System.lineSeparator();
            Files.writeString(file, line, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }

    public List<Message> loadMessages(String topic) throws IOException {
        Path file = Paths.get(DATA_DIR, topic, LOG_FILE);
        List<String> lines = Files.readAllLines(file);
        List<Message> messages = new ArrayList<>();
        for (String line : lines) {
            String[] parts = line.split("\\|", 3);
            messages.add(new Message(Long.parseLong(parts[0]), parts[2], Instant.parse(parts[1])));
        }
        return messages;
    }
}
