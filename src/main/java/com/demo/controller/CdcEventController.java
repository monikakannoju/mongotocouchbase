package com.demo.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import com.demo.dto.CdcEvent;

@Controller
public class CdcEventController {
    private static final Logger logger = LoggerFactory.getLogger(CdcEventController.class);

    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    // Send a generic CDC event
    public void sendCdcEvent(CdcEvent event) {
        try {
            messagingTemplate.convertAndSend("/topic/cdc-events", event);
            logger.info("✅ CDC Event sent via WebSocket: {} - {}.{} - Document: {}", 
                       event.getEventType(), event.getDatabase(), 
                       event.getCollection(), event.getDocumentId());
            logger.debug("Event details: {}", event.toString());
        } catch (Exception e) {
            logger.error("❌ Failed to send CDC event via WebSocket", e);
        }
    }
    
    // Insert document event
    public void sendInsertEvent(String database, String collection, 
                              String documentId, Map<String, Object> document) {
        CdcEvent event = new CdcEvent("INSERT", database, collection);
        event.setDocumentId(documentId);
        event.setAfter(document);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "document_insert");
        metadata.put("timestamp", System.currentTimeMillis());
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
    }
    
    // Delete document event
    public void sendDeleteEvent(String database, String collection, 
                              String documentId, Map<String, Object> document) {
        CdcEvent event = new CdcEvent("DELETE", database, collection);
        event.setDocumentId(documentId);
        event.setBefore(document);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "document_delete");
        metadata.put("timestamp", System.currentTimeMillis());
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
    }
    
    // Update document fields event
    public void sendUpdateEvent(String database, String collection, 
                              String documentId, Map<String, Object> before, 
                              Map<String, Object> after, Map<String, Object> changedFields) {
        CdcEvent event = new CdcEvent("UPDATE", database, collection);
        event.setDocumentId(documentId);
        event.setBefore(before);
        event.setAfter(after);
        event.setChangedFields(changedFields);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "document_update");
        metadata.put("timestamp", System.currentTimeMillis());
        metadata.put("changedFieldsCount", changedFields != null ? changedFields.size() : 0);
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
    }
    
    // Delete document field event
    public void sendFieldDeleteEvent(String database, String collection, 
                                   String documentId, Map<String, Object> before, 
                                   Map<String, Object> after, String deletedField) {
        CdcEvent event = new CdcEvent("FIELD_DELETE", database, collection);
        event.setDocumentId(documentId);
        event.setBefore(before);
        event.setAfter(after);
        
        Map<String, Object> changedFields = new HashMap<>();
        changedFields.put(deletedField, null);
        event.setChangedFields(changedFields);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "field_delete");
        metadata.put("deletedField", deletedField);
        metadata.put("timestamp", System.currentTimeMillis());
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
    }
    
    // ✅ ADDED: Drop collection event
    public void sendDropCollectionEvent(String database, String collection, int deletedCount) {
        CdcEvent event = new CdcEvent("DROP_COLLECTION", database, collection);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "collection_drop");
        metadata.put("deletedCount", deletedCount);
        metadata.put("timestamp", System.currentTimeMillis());
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
        logger.info("✅ CDC Event sent via WebSocket: DROP_COLLECTION - {}.{} - {} documents deleted", 
                   database, collection, deletedCount);
    }
    
    // Batch events (for efficiency when processing multiple changes)
    public void sendBatchEvents(String database, String collection, 
                               String operationType, int count) {
        CdcEvent event = new CdcEvent("BATCH_" + operationType, database, collection);
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("operation", "batch_" + operationType.toLowerCase());
        metadata.put("count", count);
        metadata.put("timestamp", System.currentTimeMillis());
        event.setMetadata(metadata);
        
        sendCdcEvent(event);
    }
}