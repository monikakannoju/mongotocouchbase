package com.demo.exception;

import com.demo.dto.ErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice; // Change this
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.ValidationException;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestControllerAdvice 
public class GlobalExceptionHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    private static final Logger securityLogger = LoggerFactory.getLogger("SECURITY"); // Add security logger

    private boolean isPotentialXssAttack(String message) {
        if (message == null) return false;
        String lowerMessage = message.toLowerCase();
        return lowerMessage.contains("<script>") || 
               lowerMessage.contains("javascript:") ||
               lowerMessage.contains("onload") ||
               lowerMessage.contains("alert(") ||
               lowerMessage.contains("eval(");
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGenericException(Exception e) {
        String errorId = UUID.randomUUID().toString();
        
        logger.error("Error ID: {} - Unhandled exception: ", errorId, e);
        
        ErrorResponse response = new ErrorResponse(
            "Internal Server Error",
            "An error occurred processing your request. Reference: " + errorId,
            null
        );
        
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
    }
    
    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(ValidationException e) {
        logger.warn("Validation error: {}", e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "Validation Error",
            e.getMessage(),
            null
        );
        
        return ResponseEntity.badRequest().body(response);
    }
    
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleMethodArgumentNotValid(
            MethodArgumentNotValidException e) {
        
        Map<String, String> errors = new HashMap<>();
        e.getBindingResult().getAllErrors().forEach((error) -> {
            String fieldName = ((FieldError) error).getField();
            String errorMessage = error.getDefaultMessage();
            errors.put(fieldName, errorMessage);
        });
        
        // Check for potential XSS attacks
        boolean potentialAttack = errors.values().stream()
                .anyMatch(this::isPotentialXssAttack);
        
        if (potentialAttack) {
            String errorId = UUID.randomUUID().toString();
            securityLogger.warn("Potential XSS attack detected [{}]: {}", errorId, errors);
        }
        
        logger.warn("Method argument validation failed: {}", errors);
        
        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", LocalDateTime.now());
        response.put("status", HttpStatus.BAD_REQUEST.value());
        response.put("error", "Validation Failed");
        response.put("errors", errors);
        
        return ResponseEntity.badRequest().body(response);
    }
    
    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException e) {
        logger.warn("Constraint violation: {}", e.getMessage());
        
        if (isPotentialXssAttack(e.getMessage())) {
            String errorId = UUID.randomUUID().toString();
            securityLogger.warn("Potential XSS attack detected [{}]: {}", errorId, e.getMessage());
        }
        
        ErrorResponse response = new ErrorResponse(
            "Constraint Violation",
            "Invalid input parameters",
            e.getMessage()
        );
        
        return ResponseEntity.badRequest().body(response);
    }
    
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<ErrorResponse> handleAccessDenied(AccessDeniedException e) {
        logger.warn("Access denied: {}", e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "Access Denied",
            "You don't have permission to access this resource",
            null
        );
        
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }
    
    @ExceptionHandler(SecurityException.class)
    public ResponseEntity<ErrorResponse> handleSecurityException(SecurityException e) {
        String errorId = UUID.randomUUID().toString();
        securityLogger.error("Security exception - Error ID: {}: {}", errorId, e.getMessage(), e);
        
        ErrorResponse response = new ErrorResponse(
            "Security Error",
            "Security validation failed. Reference: " + errorId,
            null
        );
        
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(response);
    }
    
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public ResponseEntity<ErrorResponse> handleMaxSizeException(MaxUploadSizeExceededException e) {
        logger.warn("File upload size exceeded: {}", e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "File Too Large",
            "File size exceeds maximum allowed size",
            null
        );
        
        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(response);
    }
    
//    @ExceptionHandler(ResourceLimitException.class)
//    public ResponseEntity<ErrorResponse> handleResourceLimit(ResourceLimitException e) {
//        logger.warn("Resource limit exceeded: {}", e.getMessage());
//        
//        ErrorResponse response = new ErrorResponse(
//            "Resource Limit Exceeded",
//            e.getMessage(),
//            null
//        );
//        
//        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(response);
//    }
    
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException e) {
        logger.warn("Illegal argument: {}", e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "Invalid Request",
            e.getMessage(),
            null
        );
        
        return ResponseEntity.badRequest().body(response);
    }
    
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalState(IllegalStateException e) {
        logger.warn("Illegal state: {}", e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "Invalid State",
            e.getMessage(),
            null
        );
        
        return ResponseEntity.status(HttpStatus.CONFLICT).body(response);
    }

    // Add this to handle any other security-related exceptions
    @ExceptionHandler(org.springframework.security.core.AuthenticationException.class)
    public ResponseEntity<ErrorResponse> handleAuthenticationException(
            org.springframework.security.core.AuthenticationException e) {
        String errorId = UUID.randomUUID().toString();
        securityLogger.warn("Authentication failed [{}]: {}", errorId, e.getMessage());
        
        ErrorResponse response = new ErrorResponse(
            "Authentication Failed",
            "Invalid credentials. Reference: " + errorId,
            null
        );
        
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(response);
    }
    private String sanitizeErrorMessage(String message) {
        if (message == null) return "An error occurred";
        
        if (message.contains("at ") || message.contains("Exception") || message.contains("Error")) {
            return "An internal error occurred";
        }
        
        return message;
    }
    
}