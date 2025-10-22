package com.demo.service;
 
import com.mongodb.MongoCommandException;

import com.mongodb.MongoException;

import com.mongodb.client.MongoClient;

import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Service;
 
import java.util.ArrayList;

import java.util.LinkedHashMap;

import java.util.List;

import java.util.Map;

import java.util.stream.Collectors;
 
@Service

public class MongoMetadataService {

    private static final Logger logger = LoggerFactory.getLogger(MongoMetadataService.class);

    private final MongoConnectionService mongoConnectionService;
 
    @Autowired

    public MongoMetadataService(MongoConnectionService mongoConnectionService) {

        this.mongoConnectionService = mongoConnectionService;

    }
 
    public Map<String, Object> getStorageMetrics() throws MongoMetadataException {

        MongoClient mongoClient = null;

        try {

            mongoClient = mongoConnectionService.getMongoClient();

            Map<String, Object> metrics = new LinkedHashMap<>();

            long totalDocuments = 0;

            long totalCollections = 0;

            long totalStorageBytes = 0;

            long totalDataBytes = 0;

            long totalIndexBytes = 0;

            int totalDatabases = 0;
 
            try {

                // Get all databases excluding system databases

                List<Document> databaseInfos = mongoClient.listDatabases().into(new ArrayList<>())

                    .stream()

                    .filter(dbInfo -> !isSystemDatabase(dbInfo.getString("name")))

                    .collect(Collectors.toList());
 
                for (Document dbInfo : databaseInfos) {

                    String dbName = dbInfo.getString("name");

                    totalDatabases++;

                    MongoDatabase db = mongoClient.getDatabase(dbName);

                    try {

                        // First try to get stats at database level (faster)

                        Document dbStats = db.runCommand(new Document("dbStats", 1).append("scale", 1));

                        // Get user collections count (excluding system collections)

                        long userCollectionsCount = db.listCollectionNames()

                            .into(new ArrayList<>())

                            .stream()

                            .filter(this::isUserCollection)

                            .count();

                        totalCollections += userCollectionsCount;

                        if (userCollectionsCount > 0) {

                            // If we have user collections, get their stats

                            CollectionStatsResult result = getCollectionLevelStats(db);

                            totalDocuments += result.docCount;

                            totalStorageBytes += result.storageSize;

                            totalDataBytes += result.dataSize;

                            totalIndexBytes += result.indexSize;

                        }

                    } catch (MongoCommandException e) {

                        logger.warn("Database-level stats failed for {} ({}), falling back to collection-level stats", 

                            dbName, e.getErrorMessage());

                        CollectionStatsResult result = getCollectionLevelStats(db);

                        totalDocuments += result.docCount;

                        totalCollections += result.collectionCount;

                        totalStorageBytes += result.storageSize;

                        totalDataBytes += result.dataSize;

                        totalIndexBytes += result.indexSize;

                    } catch (MongoException e) {

                        throw new MongoMetadataException("Failed to process database: " + dbName, e);

                    }

                }

            } catch (MongoException e) {

                throw new MongoMetadataException("Failed to fetch database list", e);

            }
 
            // Format results

            metrics.put("totalDatabases", totalDatabases);

            metrics.put("totalCollections", totalCollections);

            metrics.put("totalDocuments", totalDocuments);

            metrics.put("logicalDataSize", formatSize(totalDataBytes));

            metrics.put("storageSize", formatSize(totalStorageBytes));

            metrics.put("indexSize", formatSize(totalIndexBytes));

            metrics.put("compressionRatio", calculateCompressionRatio(totalDataBytes, totalStorageBytes));

            return metrics;

        } catch (Exception e) {

            logger.error("Critical error while fetching storage metrics", e);

            throw new MongoMetadataException("Failed to fetch storage metrics", e);

        }

    }
 
    private CollectionStatsResult getCollectionLevelStats(MongoDatabase db) throws MongoMetadataException {

        CollectionStatsResult result = new CollectionStatsResult();

        try {

            // Get only user collections

            List<String> collectionNames = db.listCollectionNames()

                .into(new ArrayList<>())

                .stream()

                .filter(this::isUserCollection)

                .collect(Collectors.toList());

            result.collectionCount = collectionNames.size();
 
            for (String collName : collectionNames) {

                try {

                    Document collStats = db.runCommand(new Document("collStats", collName));

                    result.docCount += getSafeLong(collStats, "count");

                    result.storageSize += getSafeLong(collStats, "storageSize");

                    result.dataSize += getSafeLong(collStats, "size");

                    result.indexSize += getSafeLong(collStats, "totalIndexSize");

                } catch (MongoCommandException e) {

                    logger.warn("Collection stats failed for {} ({}), falling back to countDocuments", 

                        collName, e.getErrorMessage());

                    try {

                        result.docCount += db.getCollection(collName).countDocuments();

                    } catch (MongoException ex) {

                        logger.error("countDocuments failed for {}: {}", collName, ex.getMessage());

                        throw new MongoMetadataException("Failed to get document count for collection: " + collName, ex);

                    }

                } catch (MongoException e) {

                    throw new MongoMetadataException("Failed to get stats for collection: " + collName, e);

                }

            }

            return result;

        } catch (MongoException e) {

            throw new MongoMetadataException("Failed to list collections", e);

        }

    }
 
    private boolean isSystemDatabase(String dbName) {

        return dbName.equals("admin") || 

               dbName.equals("local") || 

               dbName.equals("config") ||

               dbName.startsWith("system.");

    }
 
    private boolean isUserCollection(String collectionName) {

        return !collectionName.startsWith("system.") && 

               !collectionName.equals("system.profile") &&

               !collectionName.equals("system.js") &&

               !collectionName.equals("system.views");

    }
 
    private String formatSize(long bytes) {

        if (bytes < 1024) return bytes + " bytes";

        if (bytes < 1024 * 1024) return (bytes / 1024) + " KB";

        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024.0));

        return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));

    }
 
    private double calculateCompressionRatio(long logicalSize, long storageSize) {

        if (storageSize == 0) return 0;

        return Math.round((logicalSize / (double) storageSize) * 100) / 100.0;

    }
 
    private long getSafeLong(Document doc, String key) {

        try {

            Object value = doc.get(key);

            return (value instanceof Number) ? ((Number) value).longValue() : 0L;

        } catch (Exception e) {

            logger.debug("Failed to get long value for key {}: {}", key, e.getMessage());

            return 0L;

        }

    }
 
    private static class CollectionStatsResult {

        long docCount = 0;

        long storageSize = 0;

        long dataSize = 0;

        long indexSize = 0;

        int collectionCount = 0;

    }
 
    public static class MongoMetadataException extends Exception {

        public MongoMetadataException(String message) {

            super(message);

        }
 
        public MongoMetadataException(String message, Throwable cause) {

            super(message, cause);

        }

    }

}
 