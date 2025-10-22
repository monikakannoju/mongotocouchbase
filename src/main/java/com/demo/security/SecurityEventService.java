package com.demo.security;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
public class SecurityEventService {
    
    private static final Logger logger = LoggerFactory.getLogger(SecurityEventService.class);
    private static final int MAX_EVENTS = 10000;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    
    private final Deque<SecurityEvent> events = new ConcurrentLinkedDeque<>();
    private final AtomicLong eventCounter = new AtomicLong(0);
    
    public void logSecurityEvent(SecurityEventType type, String source, String description, SecuritySeverity severity) {
        SecurityEvent event = new SecurityEvent(
            eventCounter.incrementAndGet(),
            type,
            source,
            description,
            severity,
            LocalDateTime.now()
        );
        
        // Add event and maintain max size
        events.addFirst(event);
        while (events.size() > MAX_EVENTS) {
            events.removeLast();
        }
        
        // Log based on severity
        switch (severity) {
            case CRITICAL:
                logger.error("SECURITY_EVENT [{}] {} - {} from {}", severity, type, description, source);
                // In production, you might want to send alerts here
                break;
            case HIGH:
                logger.warn("SECURITY_EVENT [{}] {} - {} from {}", severity, type, description, source);
                break;
            case MEDIUM:
                logger.info("SECURITY_EVENT [{}] {} - {} from {}", severity, type, description, source);
                break;
            case LOW:
                logger.debug("SECURITY_EVENT [{}] {} - {} from {}", severity, type, description, source);
                break;
        }
    }
    
    /**
     * Get recent security events
     */
    public List<SecurityEvent> getRecentEvents(int limit) {
        return events.stream()
            .limit(Math.min(limit, MAX_EVENTS))
            .collect(Collectors.toList());
    }
    
    public List<SecurityEvent> getEventsByType(SecurityEventType type, int limit) {
        return events.stream()
            .filter(event -> event.getType() == type)
            .limit(Math.min(limit, MAX_EVENTS))
            .collect(Collectors.toList());
    }
    public List<SecurityEvent> getEventsBySeverity(SecuritySeverity severity, int limit) {
        return events.stream()
            .filter(event -> event.getSeverity() == severity)
            .limit(Math.min(limit, MAX_EVENTS))
            .collect(Collectors.toList());
    }
    public SecurityEventStats getEventStatistics() {
        Map<SecurityEventType, Long> typeCount = events.stream()
            .collect(Collectors.groupingBy(SecurityEvent::getType, Collectors.counting()));
        
        Map<SecuritySeverity, Long> severityCount = events.stream()
            .collect(Collectors.groupingBy(SecurityEvent::getSeverity, Collectors.counting()));
        
        return new SecurityEventStats(
            events.size(),
            typeCount,
            severityCount,
            events.isEmpty() ? null : events.getFirst().getTimestamp()
        );
    }
    
    /**
     * Clear all events (for testing purposes)
     */
    public void clearEvents() {
        events.clear();
        eventCounter.set(0);
        logger.info("Security events cleared");
    }
    
    public static class SecurityEvent {
        private final Long id;
        private final SecurityEventType type;
        private final String source;
        private final String description;
        private final SecuritySeverity severity;
        private final LocalDateTime timestamp;
        
        public SecurityEvent(Long id, SecurityEventType type, String source, 
                           String description, SecuritySeverity severity, LocalDateTime timestamp) {
            this.id = id;
            this.type = type;
            this.source = source;
            this.description = description;
            this.severity = severity;
            this.timestamp = timestamp;
        }
        
        // Getters
        public Long getId() { return id; }
        public SecurityEventType getType() { return type; }
        public String getSource() { return source; }
        public String getDescription() { return description; }
        public SecuritySeverity getSeverity() { return severity; }
        public LocalDateTime getTimestamp() { return timestamp; }
        
        @Override
        public String toString() {
            return String.format("SecurityEvent[id=%d, type=%s, severity=%s, source=%s, timestamp=%s, description=%s]",
                id, type, severity, source, timestamp.format(DATE_FORMATTER), description);
        }
    }
    
    public static class SecurityEventStats {
        private final long totalEvents;
        private final Map<SecurityEventType, Long> eventsByType;
        private final Map<SecuritySeverity, Long> eventsBySeverity;
        private final LocalDateTime mostRecentEvent;
        
        public SecurityEventStats(long totalEvents, Map<SecurityEventType, Long> eventsByType,
                                 Map<SecuritySeverity, Long> eventsBySeverity, LocalDateTime mostRecentEvent) {
            this.totalEvents = totalEvents;
            this.eventsByType = eventsByType;
            this.eventsBySeverity = eventsBySeverity;
            this.mostRecentEvent = mostRecentEvent;
        }
        
        // Getters
        public long getTotalEvents() { return totalEvents; }
        public Map<SecurityEventType, Long> getEventsByType() { return eventsByType; }
        public Map<SecuritySeverity, Long> getEventsBySeverity() { return eventsBySeverity; }
        public LocalDateTime getMostRecentEvent() { return mostRecentEvent; }
    }
}