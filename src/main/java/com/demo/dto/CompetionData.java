package com.demo.dto;

import java.util.Map;
import java.util.HashMap;

public class CompetionData {
    private String database;
    private String collection;
    private int transferred;
    private int total;
    private int currentTotal;
    private String status;
    private String operationType;
    private int changeCount;
    private Map<String, Object> details;
    private Long durationMs;
    private Long speed;
    
    // Constructors, getters, and setters
    public CompetionData() {
        this.details = new HashMap<>();
    }
    
    public CompetionData(String database, String collection, int transferred, 
                        int total, int currentTotal, String status, String operationType, 
                        int changeCount, Map<String, Object> details, Long durationMs, Long speed) {
        this();
        this.database = database;
        this.collection = collection;
        this.transferred = transferred;
        this.total = total;
        this.currentTotal = currentTotal;
        this.status = status;
        this.operationType = operationType;
        this.changeCount = changeCount;
        if (details != null) {
            this.details = details;
        }
        this.durationMs = durationMs;
        this.speed = speed;
    }
    
    // Getters and setters
    public String getDatabase() { return database; }
    public void setDatabase(String database) { this.database = database; }
    
    public String getCollection() { return collection; }
    public void setCollection(String collection) { this.collection = collection; }
    
    public int getTransferred() { return transferred; }
    public void setTransferred(int transferred) { this.transferred = transferred; }
    
    public int getTotal() { return total; }
    public void setTotal(int total) { this.total = total; }
    
    public int getCurrentTotal() { return currentTotal; }
    public void setCurrentTotal(int currentTotal) { this.currentTotal = currentTotal; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public String getOperationType() { return operationType; }
    public void setOperationType(String operationType) { this.operationType = operationType; }
    
    public int getChangeCount() { return changeCount; }
    public void setChangeCount(int changeCount) { this.changeCount = changeCount; }
    
    public Map<String, Object> getDetails() { return details; }
    public void setDetails(Map<String, Object> details) { this.details = details; }
    
    public Long getDurationMs() { return durationMs; }
    public void setDurationMs(Long durationMs) { this.durationMs = durationMs; }
    
    public Long getSpeed() { return speed; }
    public void setSpeed(Long speed) { this.speed = speed; }
    
    public void addDetail(String key, Object value) {
        this.details.put(key, value);
    }
}