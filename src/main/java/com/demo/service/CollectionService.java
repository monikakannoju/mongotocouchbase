package com.demo.service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CollectionService {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    // This is a dummy method to simulate data migration
    public void migrateDataWithProgress() throws InterruptedException {
        // Simulate source data
        List<String> sourceData = List.of("doc1", "doc2", "doc3", "doc4", "doc5");
        int totalCount = sourceData.size();

        for (int i = 0; i < totalCount; i++) {
            // Simulate data transfer (e.g., Mongo -> Couchbase)
            String doc = sourceData.get(i);
            // TODO: your actual migration logic here (save to Couchbase)

            // Calculate progress
            int migratedCount = i + 1;
            int percentage = (int) ((migratedCount * 100.0) / totalCount);

            // Send progress over WebSocket
            Map<String, Object> progressPayload = new HashMap<>();
            progressPayload.put("percentage", percentage);
            progressPayload.put("migratedCount", migratedCount);
            progressPayload.put("totalCount", totalCount);
            progressPayload.put("status", migratedCount == totalCount ? "COMPLETED" : "IN_PROGRESS");

            messagingTemplate.convertAndSend("/topic/migration-progress", progressPayload);

            // Simulate delay (optional)
            Thread.sleep(500);
        }
    }
}

