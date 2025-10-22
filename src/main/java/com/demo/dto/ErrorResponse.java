package com.demo.dto;
 
import java.time.LocalDateTime;
 
public class ErrorResponse {
    private LocalDateTime timestamp;
    private String message;
    private String details;
    private String error;
    
    public ErrorResponse( String message) {
        this.timestamp = LocalDateTime.now();
        this.message = message;
    }
 
    public ErrorResponse(String error,String message, String details) {
        this.timestamp = LocalDateTime.now();
        this.message = message;
        this.details = details;
        this.error = error;
    }
 
    public String getError() {
    	return error;
    	}
    
    public LocalDateTime getTimestamp() {
        return timestamp;
    }
 
    public String getMessage() {
        return message;
    }
 
    public String getDetails() {
        return details;
    }
}
 