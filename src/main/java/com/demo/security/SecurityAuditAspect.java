package com.demo.security;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.demo.annotation.Audited;
import com.fasterxml.jackson.databind.ObjectMapper;

@Aspect
@Component
public class SecurityAuditAspect {
    
    private static final Logger auditLogger = LoggerFactory.getLogger("SECURITY_AUDIT");
    private static final Logger logger = LoggerFactory.getLogger(SecurityAuditAspect.class);
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    @Around("@annotation(audited)")
    public Object auditMethod(ProceedingJoinPoint joinPoint, Audited audited) throws Throwable {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String user = auth != null ? auth.getName() : "anonymous";
        String method = joinPoint.getSignature().toShortString();
        String auditLabel = audited.value();
        
        // Log method entry
        auditLogger.info("AUDIT [{}] - User: {} accessing method: {} with args: {}", 
            auditLabel, user, method, sanitizeArgs(joinPoint.getArgs()));
        
        long startTime = System.currentTimeMillis();
        
        try {
            Object result = joinPoint.proceed();
            long duration = System.currentTimeMillis() - startTime;
            
            // Log successful completion
            auditLogger.info("AUDIT [{}] - User: {} successfully completed: {} in {}ms", 
                auditLabel, user, method, duration);
            
            return result;
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            
            // Log failure
            auditLogger.error("AUDIT [{}] - User: {} failed at: {} after {}ms - Error: {}", 
                auditLabel, user, method, duration, e.getMessage());
            
            throw e;
        }
    }
    
    private String sanitizeArgs(Object[] args) {
        if (args == null || args.length == 0) {
            return "[]";
        }
        
        return Arrays.stream(args)
            .map(this::sanitizeArg)
            .collect(Collectors.joining(", ", "[", "]"));
    }
    
    private String sanitizeArg(Object arg) {
        if (arg == null) {
            return "null";
        }
        
        String className = arg.getClass().getSimpleName();
        
        // Don't log sensitive data
        if (className.contains("Password") || 
            className.contains("Credential") || 
            className.contains("Token") ||
            className.contains("Secret")) {
            return className + "[REDACTED]";
        }
        
        // For file uploads, just log the filename
        if (arg instanceof org.springframework.web.multipart.MultipartFile) {
            org.springframework.web.multipart.MultipartFile file = 
                (org.springframework.web.multipart.MultipartFile) arg;
            return "MultipartFile[name=" + file.getOriginalFilename() + 
                   ", size=" + file.getSize() + "]";
        }
        
        // For complex objects, log type and hashcode
        if (arg instanceof com.demo.dto.MongoConnectionDetails) {
            return "MongoConnectionDetails[REDACTED]";
        }
        
        if (arg instanceof com.demo.dto.CouchbaseConnectionDetails) {
            return "CouchbaseConnectionDetails[REDACTED]";
        }
        
        // For simple types, log the value
        if (arg instanceof String || arg instanceof Number || arg instanceof Boolean) {
            String value = arg.toString();
            // Truncate long strings
            if (value.length() > 100) {
                return value.substring(0, 97) + "...";
            }
            return value;
        }
        
        // For other objects, just log the class name
        return className + "@" + Integer.toHexString(arg.hashCode());
    }
 // Add these methods for better auditing
    @Around("execution(* com.demo.controller..*(..))")
    public Object auditControllerMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        String methodName = joinPoint.getSignature().getName();
        String className = joinPoint.getTarget().getClass().getSimpleName();
        
        auditLogger.info("CONTROLLER_ACCESS - Class: {}, Method: {}, User: {}", 
            className, methodName, getCurrentUser());
        
        return joinPoint.proceed();
    }

    private String getCurrentUser() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        return auth != null ? auth.getName() : "anonymous";
    }
}