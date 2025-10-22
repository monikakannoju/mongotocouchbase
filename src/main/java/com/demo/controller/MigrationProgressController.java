
package com.demo.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.dto.MigrationProgress;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
public class MigrationProgressController {

    private static final Logger logger = LoggerFactory.getLogger(MigrationProgressController.class);

    private static final String PROGRESS_TOPIC = "migration-progress";
    private static final String WEBSOCKET_TOPIC = "/topic/migration-progress";
    private static final String CDC_TOPIC = "/topic/cdc-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    private final ConcurrentHashMap<String, Integer> currentTotals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> startTimes = new ConcurrentHashMap<>();

    @Autowired
    public MigrationProgressController(KafkaTemplate<String, String> kafkaTemplate,
                                      SimpMessagingTemplate messagingTemplate,
                                      ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    // Test endpoint for drop events
    @GetMapping("/test-drop")
    public String testDropEvent() {
        String database = "your-database-name";
        String collection = "your-collection-name";
        String droppedCollection = "products";
        int deletedCount = 15;
        
        sendDropEvent(database, collection, droppedCollection, deletedCount);
        
        return "Test drop event sent! Check frontend and backend logs.";
    }

    /**
     * Send progress update for migration
     */
    public void sendProgressUpdate(String database, String collection, 
                                  int transferred, int total, int currentTotal,
                                  String status, String operationType, 
                                  int changeCount, Map<String, Object> details,
                                  long durationMs, long speed) {
        
        String collectionKey = database + "." + collection;
        
        if (currentTotal >= 0) {
            currentTotals.put(collectionKey, currentTotal);
        } else {
            currentTotal = currentTotals.getOrDefault(collectionKey, total);
        }

        long actualDurationMs = durationMs;
        
        if (durationMs == 0 && transferred > 0) {
            Long startTime = startTimes.get(collectionKey);
            if (startTime != null) {
                actualDurationMs = System.currentTimeMillis() - startTime;
            }
        }
        
        if ("STARTED".equals(status) || "INITIALIZED".equals(status)) {
            startTimes.put(collectionKey, System.currentTimeMillis());
        }
        
        if ("COMPLETED".equals(status)) {
            startTimes.remove(collectionKey);
        }

        MigrationProgress progress = new MigrationProgress(
            database, 
            collection, 
            transferred, 
            total, 
            currentTotal,
            status, 
            operationType, 
            changeCount, 
            details, 
            actualDurationMs,
            null, 
            speed
        );

        try {
            String json = objectMapper.writeValueAsString(progress);
            kafkaTemplate.send(PROGRESS_TOPIC, json);
        } catch (JsonProcessingException e) {
            logger.error("[ERROR] Failed to serialize progress for Kafka: {}", e.getMessage());
        }

        try {
            messagingTemplate.convertAndSend(WEBSOCKET_TOPIC, progress);
        } catch (Exception e) {
            logger.error("[ERROR] Failed to send progress via WebSocket: {}", e.getMessage());
        }

        logger.info("[PROGRESS] {}.{}: {}/{}/{} ({}), {}ms, {}/s", 
                   database, collection, transferred, total, currentTotal, 
                   status, actualDurationMs, speed);
    }

    /**
     * Overloaded method for simpler calls
     */
    public void sendProgressUpdate(String database, String collection, 
                                  int transferred, int total, String status) {
        String collectionKey = database + "." + collection;
        int currentTotal = currentTotals.getOrDefault(collectionKey, total);
        
        long durationMs = 0L;
        Long startTime = startTimes.get(collectionKey);
        if (startTime != null && transferred > 0) {
            durationMs = System.currentTimeMillis() - startTime;
        }
        
        long speed = 0L;
        if (durationMs > 0) {
            speed = (transferred * 1000L) / durationMs;
        }
        
        if ("STARTED".equals(status)) {
            startTimes.put(collectionKey, System.currentTimeMillis());
        }
        
        sendProgressUpdate(
            database, collection, 
            transferred, total, currentTotal,
            status, "MIGRATION", 
            0, null, durationMs, speed
        );
    }

    /**
     * Send initial document count for a collection
     */
    public void sendInitialCount(String database, String collection, int initialCount) {
        String collectionKey = database + "." + collection;
        currentTotals.put(collectionKey, initialCount);
        startTimes.put(collectionKey, System.currentTimeMillis());
        
        Map<String, Object> details = new HashMap<>();
        details.put("event", "initial_count");
        details.put("initialCount", initialCount);

        sendProgressUpdate(
            database, collection, 
            0, initialCount, initialCount,
            "INITIALIZED", "MIGRATION", 
            0, details, 0L, 0L
        );
    }

    /**
     * Update current total for a collection
     */
    public void updateCurrentTotal(String database, String collection, int newTotal) {
        String collectionKey = database + "." + collection;
        currentTotals.put(collectionKey, newTotal);
    }

    /**
     * Send insert event during CDC
     */
    public void sendInsertEvent(String database, String collection, 
                               int transferred, int currentTotal, int insertCount) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "insert");
        details.put("insertCount", insertCount);
        details.put("operation", "CDC");

        String collectionKey = database + "." + collection;
        long durationMs = 0L;
        Long startTime = startTimes.get(collectionKey);
        if (startTime != null) {
            durationMs = System.currentTimeMillis() - startTime;
        }
        
        long speed = 0L;
        if (durationMs > 0) {
            speed = (transferred * 1000L) / durationMs;
        }

        sendProgressUpdate(
            database, collection, 
            transferred, currentTotal, currentTotal,
            "CDC_INSERT", "CDC", 
            insertCount, details, durationMs, speed
        );
    }

    /**
     * Send delete event during CDC
     */
    public void sendDeleteEvent(String database, String collection, 
                               int transferred, int currentTotal, int deleteCount) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "delete");
        details.put("deleteCount", deleteCount);
        details.put("operation", "CDC");

        String collectionKey = database + "." + collection;
        long durationMs = 0L;
        Long startTime = startTimes.get(collectionKey);
        if (startTime != null) {
            durationMs = System.currentTimeMillis() - startTime;
        }
        
        long speed = 0L;
        if (durationMs > 0) {
            speed = (transferred * 1000L) / durationMs;
        }

        sendProgressUpdate(
            database, collection, 
            transferred, currentTotal, currentTotal,
            "CDC_DELETE", "CDC", 
            deleteCount, details, durationMs, speed
        );
    }

    /**
     * Send insert event with data for frontend CDC events
     */
    public void sendInsertEventWithData(String database, String collection, 
                                       int transferred, int currentTotal, int insertCount,
                                       String documentId, Map<String, Object> documentData) {
        sendInsertEvent(database, collection, transferred, currentTotal, insertCount);
        sendCdcEvent(database, collection, "INSERT", documentId, documentData);
    }

    /**
     * Send delete event with data for frontend CDC events
     */
    public void sendDeleteEventWithData(String database, String collection, 
                                       int transferred, int currentTotal, int deleteCount,
                                       String documentId, Map<String, Object> documentData) {
        sendDeleteEvent(database, collection, transferred, currentTotal, deleteCount);
        sendCdcEvent(database, collection, "DELETE", documentId, documentData);
    }

    /**
     * Send drop event notification
     */
    public void sendDropEvent(String database, String collection, 
                             String droppedCollection, int deletedCount) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "drop");
        details.put("droppedCollection", droppedCollection);
        details.put("deletedCount", deletedCount);
        details.put("operation", "DROP_EVENT");

        sendProgressUpdate(
            database, collection, 
            0, 0, 0,
            "COLLECTION_DROPPED", "DROP", 
            deletedCount, details, 0L, 0L
        );

        sendCdcDropEvent(database, collection, droppedCollection, deletedCount);
    }

    /**
     * Send CDC drop event to frontend
     */
    private void sendCdcDropEvent(String database, String collection, 
                                 String droppedCollection, int deletedCount) {
        try {
            Map<String, Object> cdcEvent = new HashMap<>();
            cdcEvent.put("eventType", "DROP_COLLECTION");
            cdcEvent.put("database", database);
            cdcEvent.put("collection", collection);
            cdcEvent.put("droppedCollection", droppedCollection);
            cdcEvent.put("deletedCount", deletedCount);
            cdcEvent.put("timestamp", System.currentTimeMillis());
            cdcEvent.put("message", "Collection '" + droppedCollection + 
                             "' was dropped with " + deletedCount + " documents deleted");

            messagingTemplate.convertAndSend(CDC_TOPIC, cdcEvent);
            
            logger.info("[DROP_EVENT] {}.{}: Dropped '{}', deleted {} documents", 
                       database, collection, droppedCollection, deletedCount);
            
        } catch (Exception e) {
            logger.error("[ERROR] Failed to send CDC drop event: {}", e.getMessage());
        }
    }

    /**
     * Send CDC event to frontend
     */
    public void sendCdcEvent(String database, String collection, 
                            String eventType, String documentId, 
                            Map<String, Object> data) {
        try {
            Map<String, Object> cdcEvent = new HashMap<>();
            cdcEvent.put("eventType", eventType);
            cdcEvent.put("database", database);
            cdcEvent.put("collection", collection);
            cdcEvent.put("documentId", documentId);
            cdcEvent.put("timestamp", System.currentTimeMillis());
            cdcEvent.put("data", data);
            
            messagingTemplate.convertAndSend(CDC_TOPIC, cdcEvent);
            
            logger.info("[CDC] {}.{}: {} - {}", database, collection, eventType, documentId);
        } catch (Exception e) {
            logger.error("[ERROR] Failed to send CDC event: {}", e.getMessage());
        }
    }

    /**
     * Send completion event with duration and speed
     */
    public void sendCompletionEvent(String database, String collection, 
                                   int totalTransferred, long durationMs, long speed) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "completion");
        details.put("durationMs", durationMs);
        details.put("speed", speed);
        details.put("documentsPerSecond", speed);

        sendProgressUpdate(
            database, collection, 
            totalTransferred, totalTransferred, totalTransferred,
            "COMPLETED", "MIGRATION", 
            0, details, durationMs, speed
        );

        String collectionKey = database + "." + collection;
        currentTotals.remove(collectionKey);
        startTimes.remove(collectionKey);
    }

    /**
     * Send error event
     */
    public void sendErrorEvent(String database, String collection, 
                              String errorMessage, String errorType) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "error");
        details.put("errorMessage", errorMessage);
        details.put("errorType", errorType);

        String collectionKey = database + "." + collection;
        int currentTotal = currentTotals.getOrDefault(collectionKey, 0);

        sendProgressUpdate(
            database, collection, 
            0, currentTotal, currentTotal,
            "ERROR", "MIGRATION", 
            0, details, 0L, 0L
        );
    }

    /**
     * Send connection lost event
     */
    public void sendConnectionLostEvent(String database, String collection, 
                                       int currentTransferred) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "connection_lost");
        details.put("reconnecting", true);

        String collectionKey = database + "." + collection;
        int currentTotal = currentTotals.getOrDefault(collectionKey, 0);

        sendProgressUpdate(
            database, collection, 
            currentTransferred, currentTotal, currentTotal,
            "CONNECTION_LOST", "MIGRATION", 
            0, details, 0L, 0L
        );
    }

    /**
     * Send connection restored event
     */
    public void sendConnectionRestoredEvent(String database, String collection, 
                                           int currentTransferred) {
        Map<String, Object> details = new HashMap<>();
        details.put("event", "connection_restored");
        details.put("reconnected", true);

        String collectionKey = database + "." + collection;
        int currentTotal = currentTotals.getOrDefault(collectionKey, 0);

        sendProgressUpdate(
            database, collection, 
            currentTransferred, currentTotal, currentTotal,
            "RESUMED", "MIGRATION", 
            0, details, 0L, 0L
        );
    }

    /**
     * Get current total for a collection
     */
    public int getCurrentTotal(String database, String collection) {
        String collectionKey = database + "." + collection;
        return currentTotals.getOrDefault(collectionKey, 0);
    }

    /**
     * Reset tracking for a collection
     */
    public void resetCollection(String database, String collection) {
        String collectionKey = database + "." + collection;
        currentTotals.remove(collectionKey);
        startTimes.remove(collectionKey);
    }
}
