package com.demo.security;

/**
 * Enumeration representing the severity levels of security events
 */
public enum SecuritySeverity {
    
    /**
     * Critical severity - Immediate action required
     * Examples: System compromise, data breach, complete authentication bypass
     */
    CRITICAL(4, "Critical", "Requires immediate attention and response"),
    
    /**
     * High severity - Significant security concern
     * Examples: Multiple failed login attempts, privilege escalation attempts, unauthorized access to sensitive data
     */
    HIGH(3, "High", "Significant security issue requiring prompt investigation"),
    
    /**
     * Medium severity - Notable security event
     * Examples: Single failed login, expired sessions, configuration changes
     */
    MEDIUM(2, "Medium", "Notable security event that should be monitored"),
    
    /**
     * Low severity - Informational security event
     * Examples: Successful logins, routine security checks, normal system operations
     */
    LOW(1, "Low", "Informational security event for audit purposes"),
    
    /**
     * Info severity - General information
     * Examples: System status updates, security configuration loads
     */
    INFO(0, "Info", "General security information");
    
    private final int level;
    private final String label;
    private final String description;
    
    SecuritySeverity(int level, String label, String description) {
        this.level = level;
        this.label = label;
        this.description = description;
    }
    
    /**
     * Get the numeric level of the severity
     * @return severity level (0-4)
     */
    public int getLevel() {
        return level;
    }
    
    /**
     * Get the display label for the severity
     * @return severity label
     */
    public String getLabel() {
        return label;
    }
    
    /**
     * Get the description of the severity level
     * @return severity description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Check if this severity is higher than another
     * @param other the other severity to compare
     * @return true if this severity is higher
     */
    public boolean isHigherThan(SecuritySeverity other) {
        return this.level > other.level;
    }
    
    /**
     * Check if this severity is lower than another
     * @param other the other severity to compare
     * @return true if this severity is lower
     */
    public boolean isLowerThan(SecuritySeverity other) {
        return this.level < other.level;
    }
    
    /**
     * Check if this severity is at least as high as another
     * @param minSeverity the minimum severity to check against
     * @return true if this severity meets or exceeds the minimum
     */
    public boolean meetsMinimum(SecuritySeverity minSeverity) {
        return this.level >= minSeverity.level;
    }
    
    /**
     * Get severity by its numeric level
     * @param level the numeric level
     * @return the corresponding severity, or INFO if not found
     */
    public static SecuritySeverity fromLevel(int level) {
        for (SecuritySeverity severity : values()) {
            if (severity.level == level) {
                return severity;
            }
        }
        return INFO;
    }
    
    @Override
    public String toString() {
        return String.format("%s (%d): %s", label, level, description);
    }
}