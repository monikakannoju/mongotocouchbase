//package com.demo.config;
//
//import com.demo.dto.CouchbaseConnectionDetails;
//import com.demo.dto.MongoConnectionDetails;
//import org.springframework.stereotype.Component;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//@Component
//public class TemporaryConnectionCache {
//    private final Map<String, Object> connectionStore = new ConcurrentHashMap<>();
//
//    public void store(String userId, String connectionType, Object connectionDetails) {
//        String key = userId + ":" + connectionType;
//        connectionStore.put(key, connectionDetails);
//    }
//
//    public MongoConnectionDetails getMongoConnection(String userId) {
//        return (MongoConnectionDetails) connectionStore.get(userId + ":mongo");
//    }
//
//    public CouchbaseConnectionDetails getCouchbaseConnection(String userId) {
//        return (CouchbaseConnectionDetails) connectionStore.get(userId + ":couchbase");
//    }
//
//    public void clear(String userId, String connectionType) {
//        connectionStore.remove(userId + ":" + connectionType);
//    }
//}