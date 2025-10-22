package com.demo.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final String TOPIC = "mongo-metadata";

    @Autowired
    private ObjectMapper objectMapper;

    public void sendMetadata(Object metadata) {
        try {
            String json = objectMapper.writeValueAsString(metadata);
            kafkaTemplate.send(TOPIC, json);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send metadata to Kafka", e);
        }
    }
}
