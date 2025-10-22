package com.demo.security;

import com.demo.dto.CouchbaseConnectionDetails;
import com.demo.dto.MongoConnectionDetails;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class SecureConnectionCache {
    
    private final Map<String, EncryptedConnection> connectionStore = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanupExecutor = Executors.newScheduledThreadPool(1);
    
    @Autowired
    private EncryptionService encryptionService;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    @PostConstruct
    public void init() {
        cleanupExecutor.scheduleAtFixedRate(this::cleanupExpired, 5, 5, TimeUnit.MINUTES);
    }
    
    @PreDestroy
    public void destroy() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    public void store(String userId, String connectionType, Object connectionDetails) {
        try {
            String key = userId + ":" + connectionType;
            String json = objectMapper.writeValueAsString(connectionDetails);
            String encrypted = encryptionService.encryptSensitive(json);
            
            long expiryTime = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1); // 1 hour TTL
            connectionStore.put(key, new EncryptedConnection(encrypted, expiryTime));
        } catch (Exception e) {
            throw new RuntimeException("Failed to store connection details", e);
        }
    }
    
    public MongoConnectionDetails getMongoConnection(String userId) {
        return getConnection(userId, "mongo", MongoConnectionDetails.class);
    }
    
    public CouchbaseConnectionDetails getCouchbaseConnection(String userId) {
        return getConnection(userId, "couchbase", CouchbaseConnectionDetails.class);
    }
    
    private <T> T getConnection(String userId, String connectionType, Class<T> clazz) {
        try {
            String key = userId + ":" + connectionType;
            EncryptedConnection encryptedConn = connectionStore.get(key);
            
            if (encryptedConn == null) {
                return null;
            }
            if (encryptedConn.isExpired()) {
                connectionStore.remove(key);
                return null;
            }
            
            String decrypted = encryptionService.decryptSensitive(encryptedConn.getData());
            return objectMapper.readValue(decrypted, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to retrieve connection details", e);
        }
    }
    
    public void clear(String userId, String connectionType) {
        connectionStore.remove(userId + ":" + connectionType);
    }
    
    public void clearAll(String userId) {
        connectionStore.entrySet().removeIf(entry -> entry.getKey().startsWith(userId + ":"));
    }
    
    private void cleanupExpired() {
        long now = System.currentTimeMillis();
        connectionStore.entrySet().removeIf(entry -> entry.getValue().expiryTime < now);
    }
    private static class EncryptedConnection {
        private final String data;
        private final long expiryTime;
        
        public EncryptedConnection(String data, long expiryTime) {
            this.data = data;
            this.expiryTime = expiryTime;
        }
        
        public String getData() {
            return data;
        }
        
        public boolean isExpired() {
            return System.currentTimeMillis() > expiryTime;
        }
    }
}