package com.demo.security;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.util.regex.Pattern;
import java.util.List;
import java.util.Arrays;

@Service
public class InputValidationService {
    
    private static final Pattern SQL_INJECTION_PATTERN = 
        Pattern.compile("(?i)(\\b(SELECT|INSERT|UPDATE|DELETE|DROP|UNION|EXEC|ALTER|CREATE|TRUNCATE)\\b|--|;|\\/\\*)");
    
    private static final Pattern PATH_TRAVERSAL_PATTERN = 
        Pattern.compile("(\\.\\.\\/|\\.\\.\\\\)");
    
    private static final Pattern XSS_PATTERN = 
        Pattern.compile("(<script|javascript:|onload|onerror|eval\\(|alert\\()", Pattern.CASE_INSENSITIVE);
    
    private static final Pattern NO_SQL_INJECTION_PATTERN =
        Pattern.compile("(\\$where|\\$function|this\\.|sleep\\(|db\\.eval)", Pattern.CASE_INSENSITIVE);
    
    public void validateInput(String input, String fieldName) {
        if (input == null) return;
        
        if (SQL_INJECTION_PATTERN.matcher(input).find()) {
            throw new SecurityException("Potential SQL injection detected in " + fieldName);
        }
        
        if (PATH_TRAVERSAL_PATTERN.matcher(input).find()) {
            throw new SecurityException("Potential path traversal detected in " + fieldName);
        }
        
        if (XSS_PATTERN.matcher(input).find()) {
            throw new SecurityException("Potential XSS attack detected in " + fieldName);
        }
        
        if (NO_SQL_INJECTION_PATTERN.matcher(input).find()) {
            throw new SecurityException("Potential NoSQL injection detected in " + fieldName);
        }
    }
    
    public void validateFilename(String filename) {
        if (filename == null || filename.isEmpty()) {
            throw new SecurityException("Filename cannot be empty");
        }
        
        if (filename.contains("..") || filename.contains("/") || filename.contains("\\")) {
            throw new SecurityException("Invalid filename: " + filename);
        }
        
        if (!filename.matches("^[a-zA-Z0-9_.-]{1,255}$")) {
            throw new SecurityException("Invalid filename format: " + filename);
        }
    }
    
    public void validateDatabaseName(String dbName) {
        if (dbName == null || dbName.isEmpty()) {
            throw new SecurityException("Database name cannot be empty");
        }
        
        if (!dbName.matches("^[a-zA-Z0-9_-]{1,64}$")) {
            throw new SecurityException("Invalid database name: " + dbName);
        }
        
        if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
            throw new SecurityException("System database access not allowed: " + dbName);
        }
    }
    
    public void validateCollectionName(String collectionName) {
        if (collectionName == null || collectionName.isEmpty()) {
            throw new SecurityException("Collection name cannot be empty");
        }
        
        if (!collectionName.matches("^[a-zA-Z0-9_-]{1,255}$")) {
            throw new SecurityException("Invalid collection name: " + collectionName);
        }
        
        if (collectionName.startsWith("system.")) {
            throw new SecurityException("System collection access not allowed: " + collectionName);
        }
    }
}