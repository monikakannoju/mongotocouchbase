//package com.demo.exception;
//
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.ExceptionHandler;
//import org.springframework.web.bind.annotation.RestControllerAdvice;
//
//import com.demo.dto.ErrorResponse;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import java.util.UUID;
//
//@RestControllerAdvice
//public class GlobalSecurityExceptionHandler {
//    
//    private static final Logger securityLogger = LoggerFactory.getLogger("SECURITY");
//    @ExceptionHandler(SecurityException.class)
//    public ResponseEntity<ErrorResponse> handleSecurityException(SecurityException e) {
//        String errorId = UUID.randomUUID().toString();
//        securityLogger.error("Security exception [{}]: {}", errorId, e.getMessage(), e);
//        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
//            .body(new ErrorResponse(
//                "Security validation failed",
//                "Request failed security validation. Reference: " + errorId,
//                null
//            ));
//    }
//    
//    @ExceptionHandler(Exception.class)
//    public ResponseEntity<ErrorResponse> handleGenericException(Exception e) {
//        String errorId = UUID.randomUUID().toString();
//        securityLogger.error("Unexpected error [{}]: {}", errorId, e.getMessage(), e);
//         return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//            .body(new ErrorResponse(
//                "Internal server error",
//                "An error occurred. Reference: " + errorId,
//                null
//            ));
//    }
//}