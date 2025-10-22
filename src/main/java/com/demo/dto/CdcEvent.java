// CdcEvent.java
package com.demo.dto;

import java.time.LocalDateTime;
import java.util.Map;

public class CdcEvent {
    private String eventType; // INSERT, UPDATE, DELETE, DROP, FIELD_UPDATE, FIELD_DELETE
    private String database;
    private String collection;
    private String documentId;
    private LocalDateTime timestamp;
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> changedFields;
    private Map<String, Object> metadata;
    
    // Constructors
    public CdcEvent() {
        this.timestamp = LocalDateTime.now();
    }
    
    public CdcEvent(String eventType, String database, String collection) {
        this();
        this.eventType = eventType;
        this.database = database;
        this.collection = collection;
    }
    
    // Getters and setters
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    
    public String getCollection() { return collection; }
    public void setCollection(String collection) { this.collection = collection; }
    
    public String getDocumentId() { return documentId; }
    public void setDocumentId(String documentId) { this.documentId = documentId; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public Map<String, Object> getBefore() { return before; }
    public void setBefore(Map<String, Object> before) { this.before = before; }
    
    public Map<String, Object> getAfter() { return after; }
    public void setAfter(Map<String, Object> after) { this.after = after; }
    
    public Map<String, Object> getChangedFields() { return changedFields; }
    public void setChangedFields(Map<String, Object> changedFields) { this.changedFields = changedFields; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
    
    // Helper methods
    public void addMetadata(String key, Object value) {
        if (this.metadata == null) {
            this.metadata = new java.util.HashMap<>();
        }
        this.metadata.put(key, value);
    }
}
