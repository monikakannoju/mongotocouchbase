package com.demo.dto;

public class DeltaMigration {
    private String id;
    private String time;
    private String task;
    private String details;
    private String status; // pending, in-progress, completed, failed
    private int progress;
    private String connectionStatus;
    
    // Constructors, getters, and setters
    public DeltaMigration() {}
    
    public DeltaMigration(String id, String time, String task, String details, String status, int progress, String connectionStatus) {
        this.id = id;
        this.time = time;
        this.task = task;
        this.details = details;
        this.status = status;
        this.progress = progress;
        this.connectionStatus = connectionStatus;
    }
    
    // Getters and setters for all fields
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    
    public String getTime() { return time; }
    public void setTime(String time) { this.time = time; }
    
    public String getTask() { return task; }
    public void setTask(String task) { this.task = task; }
    
    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }
    
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    
    public int getProgress() { return progress; }
    public void setProgress(int progress) { this.progress = progress; }
    
    public String getConnectionStatus() { return connectionStatus; }
    public void setConnectionStatus(String connectionStatus) { this.connectionStatus = connectionStatus; }
}