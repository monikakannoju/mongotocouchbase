
package com.demo.dto;

import java.util.Map;

public class MigrationProgress {
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
    private Double preciseDurationSeconds;
    private Long speed;

    // Constructors
    public MigrationProgress() {}

    public MigrationProgress(String database, String collection, int transferred, int total, 
                           int currentTotal, String status, String operationType, int changeCount, 
                           Map<String, Object> details, Long durationMs, Double preciseDurationSeconds, Long speed) {
        this.database = database;
        this.collection = collection;
        this.transferred = transferred;
        this.total = total;
        this.currentTotal = currentTotal;
        this.status = status;
        this.operationType = operationType;
        this.changeCount = changeCount;
        this.details = details;
        this.durationMs = durationMs;
        this.preciseDurationSeconds = preciseDurationSeconds;
        this.speed = speed;
    }

    // Getters and Setters
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

    public Double getPreciseDurationSeconds() { return preciseDurationSeconds; }
    public void setPreciseDurationSeconds(Double preciseDurationSeconds) { this.preciseDurationSeconds = preciseDurationSeconds; }

    public Long getSpeed() { return speed; }
    public void setSpeed(Long speed) { this.speed = speed; }

    // Helper method to format duration for display
    public String getFormattedDuration() {
        if (preciseDurationSeconds != null) {
            return String.format("%.3fs", preciseDurationSeconds);
        } else if (durationMs != null) {
            return String.format("%.3fs", durationMs / 1000.0);
        }
        return "0s";
    }

    // Helper method to format numbers with Indian-style commas
    public String getFormattedNumber(int number) {
        String numStr = String.valueOf(number);
        if (numStr.length() <= 3) return numStr;
        
        StringBuilder formatted = new StringBuilder();
        int count = 0;
        for (int i = numStr.length() - 1; i >= 0; i--) {
            formatted.insert(0, numStr.charAt(i));
            count++;
            if (count == 3 && i > 0) {
                formatted.insert(0, ",");
                count = 0;
            }
        }
        return formatted.toString();
    }

    @Override
    public String toString() {
        return "MigrationProgress{" +
                "database='" + database + '\'' +
                ", collection='" + collection + '\'' +
                ", transferred=" + transferred +
                ", total=" + total +
                ", currentTotal=" + currentTotal +
                ", status='" + status + '\'' +
                ", operationType='" + operationType + '\'' +
                ", changeCount=" + changeCount +
                ", durationMs=" + durationMs +
                ", preciseDurationSeconds=" + preciseDurationSeconds +
                ", speed=" + speed +
                '}';
    }
}


