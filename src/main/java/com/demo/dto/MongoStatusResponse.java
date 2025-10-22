package com.demo.dto;
 
public class MongoStatusResponse {
    private boolean connected;
    private MongoConnectionDetails connectionDetails;
 
    // Getters and Setters
    public boolean isConnected() { return connected; }
    public void setConnected(boolean connected) { this.connected = connected; }
    public MongoConnectionDetails getConnectionDetails() { return connectionDetails; }
    public void setConnectionDetails(MongoConnectionDetails connectionDetails) {
        this.connectionDetails = connectionDetails;
    }
}