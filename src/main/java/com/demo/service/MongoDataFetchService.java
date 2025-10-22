package com.demo.service;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class MongoDataFetchService {
    private final MongoConnectionService mongoConnectionService;

    @Autowired
    public MongoDataFetchService(MongoConnectionService mongoConnectionService) {
        this.mongoConnectionService = mongoConnectionService;
    }

    // <-- Added method to expose MongoClient -->
    public MongoClient getMongoClient() {
        return mongoConnectionService.getMongoClient();
    }

    @SuppressWarnings("unchecked")
    @Retryable(value = {Exception.class}, maxAttempts = 3)
    public List<Map<String, Object>> fetchDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        MongoTemplate template = new MongoTemplate(mongoClient, dbName);
        return (List<Map<String, Object>>) (List<?>) template.findAll(Map.class, collectionName);
    }

    @Retryable(value = {Exception.class}, maxAttempts = 3)
    public Stream<Map<String, Object>> streamDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        return StreamSupport.stream(collection.find().spliterator(), false)
                .map(document -> (Map<String, Object>) document);
    }

    public long countDocuments(String dbName, String collectionName) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        return mongoClient.getDatabase(dbName)
                          .getCollection(collectionName)
                          .countDocuments();
    }

    public List<Map<String, Object>> fetchBatch(String dbName, String collectionName, int skip, int limit) {
        MongoCollection<Document> collection = mongoConnectionService
                .getMongoClient()
                .getDatabase(dbName)
                .getCollection(collectionName);

        return collection.find()
                .skip(skip)
                .limit(limit)
                .into(new ArrayList<>())
                .stream()
                .map(doc -> (Map<String, Object>) doc)
                .toList();
    }

    /**
     * Simple healthcheck: runs a ping command on the admin database.
     * Throws Exception if MongoDB server is not reachable.
     */
    public void ping() {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        try {
            MongoDatabase db = mongoClient.getDatabase("admin");
            Document commandResult = db.runCommand(new Document("ping", 1));
            // If "ok" field is not 1.0, treat as failure
            Object ok = commandResult.get("ok");
            if (!(ok instanceof Number) || ((Number) ok).doubleValue() != 1.0) {
                throw new MongoException("MongoDB ping failed: " + commandResult.toJson());
            }
        } catch (Exception ex) {
            throw new MongoException("MongoDB ping failed: " + ex.getMessage(), ex);
        }
    }

    // ✅ ADDED METHOD: Fetch document by ID
    public Map<String, Object> fetchDocumentById(String dbName, String collectionName, String documentId) {
        MongoClient mongoClient = mongoConnectionService.getMongoClient();
        MongoDatabase database = mongoClient.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        
        Document result = collection.find(new Document("_id", documentId)).first();
        
        if (result != null) {
            return (Map<String, Object>) result;
        }
        return null;
    }
}