package com.demo.security;
public enum SecurityEventType {
    
    // Authentication Events
    LOGIN_SUCCESS("Successful user login"),
    LOGIN_FAILURE("Failed login attempt"),
    LOGOUT("User logout"),
    TOKEN_EXPIRED("Authentication token expired"),
    TOKEN_INVALID("Invalid authentication token"),
    
    // Authorization Events
    ACCESS_DENIED("Access denied to resource"),
    UNAUTHORIZED_ACCESS("Unauthorized access attempt"),
    PRIVILEGE_ESCALATION("Privilege escalation attempt"),
    ROLE_CHANGED("User role changed"),
    
    // Account Security Events
    ACCOUNT_LOCKED("Account locked due to security policy"),
    ACCOUNT_UNLOCKED("Account unlocked"),
    PASSWORD_CHANGED("Password changed"),
    PASSWORD_RESET("Password reset requested"),
    ACCOUNT_CREATED("New account created"),
    ACCOUNT_DELETED("Account deleted"),
    ACCOUNT_DISABLED("Account disabled"),
    ACCOUNT_ENABLED("Account enabled"),
    
    // Session Security Events
    SESSION_CREATED("New session created"),
    SESSION_EXPIRED("Session expired"),
    SESSION_INVALIDATED("Session invalidated"),
    CONCURRENT_SESSION("Concurrent session detected"),
    SESSION_HIJACK_ATTEMPT("Possible session hijacking attempt"),
    
    // Data Security Events
    DATA_ACCESS("Sensitive data accessed"),
    DATA_MODIFICATION("Sensitive data modified"),
    DATA_DELETION("Sensitive data deleted"),
    DATA_EXPORT("Data exported"),
    DATA_ENCRYPTION_FAILURE("Data encryption failed"),
    DATA_DECRYPTION_FAILURE("Data decryption failed"),
    
    // Security Policy Events
    RATE_LIMIT_EXCEEDED("Rate limit exceeded"),
    BRUTE_FORCE_ATTEMPT("Brute force attack detected"),
    SUSPICIOUS_ACTIVITY("Suspicious activity detected"),
    SECURITY_VIOLATION("Security policy violation"),
    
    // System Security Events
    SECURITY_CONFIG_CHANGED("Security configuration changed"),
    AUDIT_LOG_TAMPER("Audit log tampering detected"),
    SYSTEM_INTEGRITY_CHECK("System integrity check performed"),
    CERTIFICATE_EXPIRED("Security certificate expired"),
    CERTIFICATE_INVALID("Invalid security certificate"),
    
    // API Security Events
    API_KEY_CREATED("API key created"),
    API_KEY_REVOKED("API key revoked"),
    API_RATE_LIMIT("API rate limit reached"),
    INVALID_API_REQUEST("Invalid API request"),
    
    // Network Security Events
    IP_BLOCKED("IP address blocked"),
    IP_WHITELISTED("IP address whitelisted"),
    CORS_VIOLATION("CORS policy violation"),
    CSRF_ATTEMPT("CSRF attack attempt"),
    XSS_ATTEMPT("XSS attack attempt"),
    SQL_INJECTION_ATTEMPT("SQL injection attempt");
    
    private final String description;
    
    SecurityEventType(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return description;
    }
    
    @Override
    public String toString() {
        return name() + ": " + description;
    }
}