package com.demo.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.query.QueryOptions;
import com.demo.controller.MigrationProgressController;
import com.demo.dto.MigrationProgress;
import com.demo.util.DataTransformationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@Service
public class DataTransferService {

    private static final Logger logger = LoggerFactory.getLogger(DataTransferService.class);

    private static final int BATCH_SIZE = 4000;        
    private static final int CONCURRENCY_LEVEL = 3000;  

    private static final String PROGRESS_TOPIC = "migration-progress";

    private final MongoDataFetchService mongoDataFetchService;
    private final CouchbaseConnectionService couchbaseConnectionService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final MigrationProgressController progressController;
    private final CheckpointService checkpointService;

    private final ConcurrentHashMap<String, Thread> changeStreams = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> cdcEnabled = new ConcurrentHashMap<>();

    // CDC tracking variables
    private final ConcurrentHashMap<String, AtomicInteger> insertedDuringMigration = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> deletedDuringMigration = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> initialDocumentCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> currentTransferredCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicInteger> migrationCurrentTotals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> migrationStartTimes = new ConcurrentHashMap<>();

    private final Object deleteLock = new Object();

    private volatile boolean paused = false;
    private volatile boolean connectionLost = false;

    // New flag to stop migration on request
    private volatile boolean stopRequested = false;

    @Autowired
    public DataTransferService(MongoDataFetchService mongoDataFetchService,
                               CouchbaseConnectionService couchbaseConnectionService,
                               KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               MigrationProgressController progressController,
                               CheckpointService checkpointService) {
        this.mongoDataFetchService = mongoDataFetchService;
        this.couchbaseConnectionService = couchbaseConnectionService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.progressController = progressController;
        this.checkpointService = checkpointService;
    }

    // API to stop full migration and CDC
    public void stopMigration() {
        stopRequested = true;
        paused = true;
        for (String key : changeStreams.keySet()) {
            cdcEnabled.put(key, false);
            Thread t = changeStreams.remove(key);
            if (t != null && t.isAlive()) {
                t.interrupt();
            }
        }
        logger.warn("[WARN] Migration and CDC stop requested by user.");
    }

    private boolean checkStopRequested() {
        if (stopRequested) {
            logger.info("[INFO] Migration stopped by user request.");
            return true;
        }
        return false;
    }

    private String normalizeMongoId(Object rawId) {
        if (rawId == null) return null;
        if (rawId instanceof ObjectId) {
            return ((ObjectId) rawId).toHexString();
        }
        String rawIdStr = rawId.toString();
        if (rawIdStr.startsWith("BsonString")) {
            int start = rawIdStr.indexOf("value='") + 7;
            int end = rawIdStr.lastIndexOf("'");
            if (start > 6 && end > start) {
                return rawIdStr.substring(start, end);
            }
        }
        if (rawIdStr.startsWith("Bson")) {
            int start = rawIdStr.indexOf("value=") + 6;
            int end = rawIdStr.indexOf("}", start);
            if (start > 5 && end > start) {
                String value = rawIdStr.substring(start, end);
                if (value.startsWith("'") && value.endsWith("'")) {
                    value = value.substring(1, value.length() - 1);
                } else if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                return value;
            }
        }
        return rawIdStr;
    }

    private boolean isReplicaSet(MongoClient mongoClient) {
        try {
            Document isMasterResult = mongoClient.getDatabase("admin").runCommand(new Document("isMaster", 1));
            Object setName = isMasterResult.get("setName");
            return (setName != null && !setName.toString().isEmpty());
        } catch (Exception ex) {
            logger.error("[ERROR] Could not determine if MongoDB is a replica set: {}", ex.getMessage());
            return false;
        }
    }

    // Starts the change stream thread for CDC
    public void startChangeStream(String databaseName, String collectionName,
                                  String bucketName, String scopeName, String targetCollectionName) {
        String streamKey = databaseName + "." + collectionName;
        if (changeStreams.containsKey(streamKey)) {
            logger.info("[INFO] Change stream already running for {}.{}", databaseName, collectionName);
            return;
        }
        cdcEnabled.put(streamKey, true);

        Thread changeStreamThread = new Thread(() -> {
            MongoClient mongoClient = mongoDataFetchService.getMongoClient();
            if (!isReplicaSet(mongoClient)) {
                logger.warn("[WARN] Change stream not supported. MongoDB is not a replica set.");
                changeStreams.remove(streamKey);
                cdcEnabled.remove(streamKey);
                return;
            }
            try {
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                var collection = database.getCollection(collectionName);
                logger.info("[INFO] Starting change stream for {}.{}", databaseName, collectionName);

                try (MongoCursor<ChangeStreamDocument<Document>> cursor =
                        collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).iterator()) {
                    while (Boolean.TRUE.equals(cdcEnabled.get(streamKey)) && cursor.hasNext()) {
                        if (checkStopRequested()) break;
                        ChangeStreamDocument<Document> change = cursor.next();
                        String opType = change.getOperationType().getValue();

                        if ("drop".equals(opType) || "dropDatabase".equals(opType)) {
                            logger.info("[INFO] Drop event detected for Mongo collection '{}'", collectionName);
                            try {
                                deleteDocumentsForMongoCollection(bucketName, scopeName, targetCollectionName, collectionName);
                            } catch (Exception e) {
                                logger.error("[ERROR] Error deleting Couchbase docs on drop for collection {}: {}", collectionName, e.getMessage());
                            }
                            cdcEnabled.put(streamKey, false);
                            changeStreams.remove(streamKey);
                            logger.info("[INFO] Change stream stopped due to drop event for {}.{}", databaseName, collectionName);
                            return;
                        } else {
                            processChangeEvent(change, bucketName, scopeName, targetCollectionName, collectionName);
                        }
                    }
                }
            } catch (MongoInterruptedException mie) {
                logger.info("[INFO] Change stream thread interrupted for {}.{}, exiting cleanly.", databaseName, collectionName);
            } catch (Exception e) {
                logger.error("[ERROR] Error in change stream for {}.{}: {}", databaseName, collectionName, e.getMessage());
            } finally {
                changeStreams.remove(streamKey);
                cdcEnabled.remove(streamKey);
            }
        });
        changeStreamThread.setName("ChangeStream-" + databaseName + "-" + collectionName);
        changeStreamThread.setDaemon(true);
        changeStreamThread.start();
        changeStreams.put(streamKey, changeStreamThread);
        logger.info("[INFO] Change stream started for {}.{}", databaseName, collectionName);

        MongoClient mongoClient = mongoDataFetchService.getMongoClient();
        listenForDropEventsSingleCollection(mongoClient, databaseName, bucketName, scopeName, targetCollectionName);
    }

    private void listenForDropEventsSingleCollection(MongoClient mongoClient, String databaseName,
                                                     String bucketName, String scopeName, String targetCollectionName) {
        String streamKey = databaseName + ".dropEvents";
        if (changeStreams.containsKey(streamKey)) {
            logger.info("[INFO] Drop events listener already running for database {}", databaseName);
            return;
        }
        cdcEnabled.put(streamKey, true);
        Thread dropThread = new Thread(() -> {
            try {
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                try (MongoCursor<ChangeStreamDocument<Document>> cursor = database.watch().iterator()) {
                    while (Boolean.TRUE.equals(cdcEnabled.get(streamKey)) && cursor.hasNext()) {
                        if (checkStopRequested()) break;
                        ChangeStreamDocument<Document> event = cursor.next();
                        String opType = event.getOperationType().getValue();
                        if ("drop".equals(opType) || "dropDatabase".equals(opType)) {
                            String droppedCollection = event.getNamespace().getCollectionName();
                            logger.warn("[WARN] Detected drop event for Mongo collection '{}'. Deleting corresponding Couchbase docs.", droppedCollection);
                            try {
                                deleteDocumentsForMongoCollection(bucketName, scopeName, targetCollectionName, droppedCollection);
                            } catch (Exception e) {
                                logger.error("[ERROR] Failed to delete docs for dropped collection '{}': {}", droppedCollection, e.getMessage());
                            }
                            cdcEnabled.put(streamKey, false);
                            changeStreams.remove(streamKey);
                            logger.info("[INFO] Drop events listener stopped for database {}", databaseName);
                            return;
                        }
                    }
                }
            } catch (MongoInterruptedException mie) {
                logger.info("[INFO] Drop event listener interrupted for DB {}, exiting cleanly.", databaseName);
            } catch (Exception ex) {
                logger.error("[ERROR] Error in drop event listener thread: {}", ex.getMessage());
            } finally {
                changeStreams.remove(streamKey);
                cdcEnabled.remove(streamKey);
            }
        });
        dropThread.setName("DropListener-" + databaseName);
        dropThread.setDaemon(true);
        dropThread.start();
        changeStreams.put(streamKey, dropThread);
    }

    public void stopChangeStream(String databaseName, String collectionName) {
        String streamKey = databaseName + "." + collectionName;
        cdcEnabled.put(streamKey, false);
        changeStreams.remove(streamKey);

        String dropStreamKey = databaseName + ".dropEvents";
        cdcEnabled.put(dropStreamKey, false);
        changeStreams.remove(dropStreamKey);

        logger.info("[INFO] Change stream stopped for {}.{}", databaseName, collectionName);
    }

    private void deleteDocumentsForMongoCollection(String bucketName, String scopeName,
                                                   String couchbaseCollectionName, String mongoCollectionName) {
        synchronized (deleteLock) {
            logger.warn("[WARN] Starting deletion in Couchbase {}/{}/{} for MongoDB collection '{}'",
                    bucketName, scopeName, couchbaseCollectionName, mongoCollectionName);
            Cluster cluster = couchbaseConnectionService.getCluster();
            String fqCollection = "`" + bucketName + "`.`" + scopeName + "`.`" + couchbaseCollectionName + "`";

            QueryOptions options = QueryOptions.queryOptions()
                    .parameters(JsonObject.create().put("mongoCollection", mongoCollectionName));

            int maxRetries = 5;
            int attempt = 0;
            while (true) {
                if (checkStopRequested()) return;
                try {
                    String countStatement = "SELECT COUNT(*) AS count FROM " + fqCollection +
                            " WHERE mongoCollection IS NOT MISSING AND mongoCollection = $mongoCollection";
                    var countResult = cluster.query(countStatement, options);
                    long count = countResult.rowsAsObject().get(0).getLong("count");
                    logger.info("[INFO] Documents found to delete for MongoDB collection '{}': {}", mongoCollectionName, count);

                    if (count == 0) {
                        logger.info("[INFO] No documents to delete for MongoDB collection '{}'. Skipping deletion.", mongoCollectionName);
                        return;
                    }

                    String deleteStatement = "DELETE FROM " + fqCollection +
                            " WHERE mongoCollection IS NOT MISSING AND mongoCollection = $mongoCollection";
                    var deleteResult = cluster.query(deleteStatement, options);
                    int deletedCount = -1;
                    if (deleteResult.metaData().metrics().isPresent()) {
                        deletedCount = (int) deleteResult.metaData().metrics().get().mutationCount();
                    }
                    progressController.sendDropEvent(
                            "", "", mongoCollectionName, deletedCount
                    );
                    logger.info("[INFO] Deleted {} documents for MongoDB collection '{}' from Couchbase collection {}/{}/{}",
                            deletedCount, mongoCollectionName, bucketName, scopeName, couchbaseCollectionName);

                    return;
                } catch (RuntimeException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof InterruptedException) {
                        logger.warn("[WARN] Delete interrupted attempt {} for {}", attempt, mongoCollectionName);
                        attempt++;
                        if (attempt > maxRetries) throw e;
                        try {
                            Thread.sleep(1000 * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (cause instanceof TimeoutException) {
                        logger.warn("[WARN] Delete timeout attempt {} for {}. Retrying...", attempt, mongoCollectionName);
                        attempt++;
                        if (attempt > maxRetries) throw e;
                        try {
                            Thread.sleep(1000 * attempt);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    public record TransferRequest(String mongoDatabase, String mongoCollection, String bucketName, String scopeName, String collectionName) {
    }

    public void transferCollectionWithDocumentCheckpoints(TransferRequest request, String checkpointId) {
        paused = false;
        connectionLost = false;
        stopRequested = false;

        Checkpoint lastCheckpoint = checkpointService.loadCheckpoint(checkpointId);
        String lastProcessedId = lastCheckpoint != null ? lastCheckpoint.getLastProcessedId() : null;
        int alreadySucceeded = lastCheckpoint != null ? lastCheckpoint.getTotalSucceeded() : 0;

        long totalDocs;
        try {
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        } catch (Exception e) {
            handleConnectionLost(request, alreadySucceeded, 0, "[ERROR] MongoDB connection lost at start.");
            waitUntilConnectionsRestored();
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        }

        Collection targetCollection;
        try {
            targetCollection = couchbaseConnectionService.getTargetCollection(request.bucketName(), request.scopeName(), request.collectionName());
        } catch (Exception e) {
            handleConnectionLost(request, alreadySucceeded, 0, "[ERROR] Couchbase connection lost at start.");
            waitUntilConnectionsRestored();
            targetCollection = couchbaseConnectionService.getTargetCollection(request.bucketName(), request.scopeName(), request.collectionName());
        }

        sendProgressUpdate(request, alreadySucceeded, (int) totalDocs, "STARTED");
        startChangeStream(request.mongoDatabase(), request.mongoCollection(), request.bucketName(), request.scopeName(), request.collectionName());

        long numBatches = (totalDocs / BATCH_SIZE) + 1;
        AtomicInteger successCounter = new AtomicInteger(alreadySucceeded);

        boolean resumeMode = lastProcessedId != null;
        boolean foundLastId = !resumeMode;

        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            if (checkStopRequested()) return;
            List<Map<String, Object>> batch = null;
            while (batch == null) {
                if (checkStopRequested()) return;
                try {
                    batch = mongoDataFetchService.fetchBatch(request.mongoDatabase(), request.mongoCollection(),
                            batchIndex * BATCH_SIZE, BATCH_SIZE);
                    if (connectionLost) {
                        sendProgressUpdate(request, successCounter.get(), (int) totalDocs, "RESUMED");
                        connectionLost = false;
                        paused = false;
                    }
                } catch (Exception ex) {
                    handleConnectionLost(request, successCounter.get(), (int) totalDocs, "[ERROR] MongoDB connection lost during batch fetch.");
                    waitUntilConnectionsRestored();
                }
            }

            if (resumeMode && !foundLastId) {
                int idx = 0;
                for (; idx < batch.size(); idx++) {
                    if (normalizeMongoId(batch.get(idx).get("_id")).equals(lastProcessedId)) {
                        idx++;
                        foundLastId = true;
                        break;
                    }
                }
                if (!foundLastId) continue;
                batch = batch.subList(idx, batch.size());
            }

            String newLastId = null;
            ReactiveCollection reactiveCollection = targetCollection.reactive();
            for (Map<String, Object> document : batch) {
                if (checkStopRequested()) return;
                boolean upsertSuccess = false;
                int retryCount = 0;
                while (!upsertSuccess && retryCount <= 5) {
                    if (paused || connectionLost) {
                        handleConnectionLost(request, successCounter.get(), (int) totalDocs, "[WARN] Conn lost during upsert doc");
                        waitUntilConnectionsRestored();
                    }
                    try {
                        String id = normalizeMongoId(document.get("_id"));
                        Map<String, Object> copy = new HashMap<>(document);
                        copy.remove("_id");
                        copy.put("mongoCollection", request.mongoCollection());
                        DataTransformationUtil.convertMongoTypes(copy);
                        JsonObject jsonDoc = JsonObject.from(copy);
                        upsertWithRetry(reactiveCollection, id, jsonDoc, 3).block();
                        newLastId = id;

                        // FIX: Only increment when actual upsert success!
                        int done = successCounter.incrementAndGet();
                        if (done % 1000 == 0 || done == totalDocs) {
                            sendProgressUpdate(request, done, (int) totalDocs, "IN_PROGRESS");
                        }
                        upsertSuccess = true;
                    } catch (Exception e) {
                        retryCount++;
                        if (e instanceof MongoTimeoutException || isRetryable(e)) {
                            paused = true;
                            connectionLost = true;
                            handleConnectionLost(request, successCounter.get(), (int) totalDocs, "[WARN] Conn lost doc upsert.");
                            waitUntilConnectionsRestored();
                        } else {
                            logger.error("[ERROR] Non-retryable error for {}, skipping", document.get("_id"));
                            upsertSuccess = true;
                            // Do NOT increment counter for skipped docs!
                        }
                    }
                }
            }

            if (newLastId != null) {
                Checkpoint cp = new Checkpoint(checkpointId, "DOCUMENT_TRANSFER", successCounter.get(), 0, successCounter.get(), 0, new HashSet<>(), new HashSet<>(), newLastId);
                checkpointService.saveCheckpoint(cp);
            }
        }
        sendProgressUpdate(request, successCounter.get(), (int) totalDocs, "COMPLETED");
        checkpointService.deleteCheckpoint(checkpointId);
    }

    public void transferCollection(TransferRequest request) {
        long startTime = System.currentTimeMillis();
        AtomicInteger counter = new AtomicInteger(0);
        stopRequested = false;
        long totalDocs;

        String migrationKey = request.mongoDatabase() + "." + request.mongoCollection();
        insertedDuringMigration.put(migrationKey, new AtomicInteger(0));
        deletedDuringMigration.put(migrationKey, new AtomicInteger(0));
        currentTransferredCounts.put(migrationKey, counter);
        migrationStartTimes.put(migrationKey, startTime);

        try {
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);

            initialDocumentCounts.put(migrationKey, (int) totalDocs);
            migrationCurrentTotals.put(migrationKey, new AtomicInteger((int) totalDocs));
            progressController.sendInitialCount(request.mongoDatabase(), request.mongoCollection(), (int) totalDocs);

        } catch (Exception e) {
            handleConnectionLost(request, counter.get(), 0, "[ERROR] Mongo conn lost at start");
            waitUntilConnectionsRestored();
            totalDocs = executeMongoOperationWithRetry(() ->
                    mongoDataFetchService.countDocuments(request.mongoDatabase(), request.mongoCollection()), 5);
        }

        Collection targetCollection;
        try {
            targetCollection = couchbaseConnectionService.getTargetCollection(request.bucketName(), request.scopeName(), request.collectionName());
        } catch (Exception e) {
            handleConnectionLost(request, counter.get(), 0, "[ERROR] Couch conn lost at start");
            waitUntilConnectionsRestored();
            targetCollection = couchbaseConnectionService.getTargetCollection(request.bucketName(), request.scopeName(), request.collectionName());
        }

        sendProgressUpdate(request, 0, (int) totalDocs, "STARTED");
        startChangeStream(request.mongoDatabase(), request.mongoCollection(), request.bucketName(), request.scopeName(), request.collectionName());

        long numBatches = (totalDocs / BATCH_SIZE) + 1;
        for (int batchIndex = 0; batchIndex < numBatches; batchIndex++) {
            if (checkStopRequested()) return;
            List<Map<String, Object>> batch = null;
            while (batch == null) {
                if (checkStopRequested()) return;
                try {
                    batch = mongoDataFetchService.fetchBatch(request.mongoDatabase(), request.mongoCollection(),
                            batchIndex * BATCH_SIZE, BATCH_SIZE);
                    if (connectionLost) {
                        sendProgressUpdate(request, counter.get(), (int) totalDocs, "RESUMED");
                        connectionLost = false;
                        paused = false;
                    }
                } catch (Exception ex) {
                    handleConnectionLost(request, counter.get(), (int) totalDocs, "[WARN] Mongo lost batch fetch");
                    waitUntilConnectionsRestored();
                }
            }
            writeBatchReactiveWithPause(batch, targetCollection, counter, request, totalDocs);
        }

        long durationMs = System.currentTimeMillis() - startTime;
        long speed = (counter.get() * 1000) / Math.max(durationMs, 1);
        if (!stopRequested) {
            sendCompletionUpdate(request, counter.get(), durationMs, speed);
        }

        insertedDuringMigration.remove(migrationKey);
        deletedDuringMigration.remove(migrationKey);
        initialDocumentCounts.remove(migrationKey);
        currentTransferredCounts.remove(migrationKey);
        migrationCurrentTotals.remove(migrationKey);
        migrationStartTimes.remove(migrationKey);
    }

    // FIX: Counting logic - increment the counter ONLY after actual upsert succeeded!
    private void writeBatchReactiveWithPause(List<Map<String, Object>> docs, Collection targetCollection,
                                             AtomicInteger counter, TransferRequest request, long totalDocs) {
        if (checkStopRequested()) return;
        ReactiveCollection reactiveCollection = targetCollection.reactive();
        Function<Map<String, Object>, Mono<MutationResult>> mapper = document -> Mono.defer(() -> {
            while (true) {
                if (paused || connectionLost || checkStopRequested()) {
                    handleConnectionLost(request, counter.get(), (int) totalDocs, "[WARN] Conn lost upsert");
                    waitUntilConnectionsRestored();
                    if (checkStopRequested()) return Mono.empty();
                }
                try {
                    String id = normalizeMongoId(document.get("_id"));
                    Map<String, Object> copy = new HashMap<>(document);
                    copy.remove("_id");
                    copy.put("mongoCollection", request.mongoCollection());
                    DataTransformationUtil.convertMongoTypes(copy);
                    JsonObject jsonDoc = JsonObject.from(copy);
                    return upsertWithRetry(reactiveCollection, id, jsonDoc, 3)
                        .doOnSuccess(r -> {
                            // FIX: only increment after success!!
                            int count = counter.incrementAndGet();
                            if (count % 1000 == 0 || count == totalDocs) {
                                sendProgressUpdate(request, count, (int) totalDocs, "IN_PROGRESS");
                            }
                        });
                } catch (Exception e) {
                    if (e instanceof MongoTimeoutException || isRetryable(e)) {
                        paused = true;
                        connectionLost = true;
                        handleConnectionLost(request, counter.get(), (int) totalDocs, "[WARN] Conn lost upsert");
                        waitUntilConnectionsRestored();
                        if (checkStopRequested()) return Mono.empty();
                    } else {
                        return Mono.empty();
                    }
                }
            }
        });
        Flux.fromIterable(docs).parallel(CONCURRENCY_LEVEL).runOn(Schedulers.boundedElastic())
                .flatMap(mapper).sequential().blockLast();
    }

    private void processChangeEvent(ChangeStreamDocument<Document> change,
                                    String bucketName, String scopeName, String couchbaseCollectionName, String mongoCollectionName) {
        if (checkStopRequested()) return;
        try {
            String databaseName = change.getNamespace().getDatabaseName();
            String migrationKey = databaseName + "." + mongoCollectionName;

            Collection target = couchbaseConnectionService.getTargetCollection(bucketName, scopeName, couchbaseCollectionName);
            ReactiveCollection reactive = target.reactive();

            switch (change.getOperationType().getValue()) {
                case "insert":
                case "update":
                case "replace":
                    if (change.getFullDocument() != null) {
                        Document doc = change.getFullDocument();
                        String id = normalizeMongoId(doc.get("_id"));
                        Map<String, Object> map = new HashMap<>(doc);
                        map.remove("_id");
                        map.put("mongoCollection", mongoCollectionName);
                        DataTransformationUtil.convertMongoTypes(map);
                        JsonObject jsonDoc = JsonObject.from(map);
                        upsertWithRetry(reactive, id, jsonDoc, 3).block();

                        if (insertedDuringMigration.containsKey(migrationKey)) {
                            insertedDuringMigration.get(migrationKey).incrementAndGet();
                            int currentTransferred = currentTransferredCounts.get(migrationKey).get();
                            int newTotal = migrationCurrentTotals.get(migrationKey).addAndGet(1);
                            progressController.updateCurrentTotal(databaseName, mongoCollectionName, newTotal);
                            progressController.sendInsertEvent(
                                    databaseName, mongoCollectionName,
                                    currentTransferred, newTotal, 1
                            );
                        }
                    }
                    break;
                case "delete":
                    processDeleteEvent(change, bucketName, scopeName, couchbaseCollectionName);
                    break;
                default:
                    logger.warn("[WARN] Unhandled change event operation: {}", change.getOperationType().getValue());
            }
        } catch (Exception e) {
            logger.error("[ERROR] Error processing change event: {}", e.getMessage());
        }
    }

    private void processDeleteEvent(ChangeStreamDocument<Document> change,
                                    String bucketName, String scopeName, String collectionName) {
        if (checkStopRequested()) return;
        try {
            if (change.getDocumentKey() != null && change.getDocumentKey().containsKey("_id")) {
                Object rawId = change.getDocumentKey().get("_id");
                String id = normalizeMongoId(rawId);
                Collection targetCollection = couchbaseConnectionService.getTargetCollection(bucketName, scopeName, collectionName);
                ReactiveCollection reactiveCollection = targetCollection.reactive();
                Optional<ExistsResult> optionalExists = reactiveCollection.exists(id).blockOptional();
                boolean exists = optionalExists.map(ExistsResult::exists).orElse(false);
                if (exists) {
                    reactiveCollection.remove(id)
                            .onErrorResume(DocumentNotFoundException.class, e -> Mono.empty())
                            .retryWhen(Retry.backoff(3, Duration.ofMillis(500))
                                    .filter(e -> !(e instanceof DocumentNotFoundException)))
                            .block();

                    String databaseName = change.getNamespace().getDatabaseName();
                    String collection = change.getNamespace().getCollectionName();
                    String migrationKey = databaseName + "." + collection;

                    if (deletedDuringMigration.containsKey(migrationKey)) {
                        deletedDuringMigration.get(migrationKey).incrementAndGet();
                        int currentTransferred = currentTransferredCounts.get(migrationKey).get();
                        int newTotal = migrationCurrentTotals.get(migrationKey).addAndGet(-1);
                        progressController.updateCurrentTotal(databaseName, collection, newTotal);
                        progressController.sendDeleteEvent(
                                databaseName, collection,
                                currentTransferred, newTotal, 1
                        );
                    }

                    logger.info("[INFO] Deleted document with ID '{}' from Couchbase due to MongoDB delete event", id);
                } else {
                    logger.info("[INFO] Document with ID '{}' does not exist in Couchbase; skipping delete", id);
                }
            }
        } catch (Exception e) {
            logger.error("[ERROR] Error deleting Couchbase document due to MongoDB delete event: {}", e.getMessage(), e);
        }
    }

    private boolean isMongoConnected() {
        try {
            mongoDataFetchService.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean isCouchbaseConnected() {
        try {
            couchbaseConnectionService.ping();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void waitUntilConnectionsRestored() {
        while (!isMongoConnected() || !isCouchbaseConnected()) {
            paused = true;
            connectionLost = true;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {
            }
            if (checkStopRequested()) return;
        }
        paused = false;
        connectionLost = false;
    }

    private void handleConnectionLost(TransferRequest req, int count, int total, String msg) {
        sendProgressUpdate(req, count, total, "CONNECTION_LOST");
        logger.warn(msg);
    }

    private <T> T executeMongoOperationWithRetry(Supplier<T> op, int maxRetries) {
        int attempts = 0;
        while (true) {
            if (checkStopRequested()) throw new IllegalStateException("Stopped by user request!");
            try {
                return op.get();
            } catch (MongoTimeoutException ex) {
                paused = true;
                connectionLost = true;
                throw ex;
            } catch (Exception ex) {
                if (isRetryable(ex)) {
                    attempts++;
                    if (attempts > maxRetries) throw ex;
                    try {
                        Thread.sleep(1000 * attempts * attempts);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    if (checkStopRequested()) throw new IllegalStateException("Stopped by user request!");
                } else throw new RuntimeException(ex);
            }
        }
    }

    private Mono<MutationResult> upsertWithRetry(ReactiveCollection collection, String id, JsonObject doc, int maxRetries) {
        return Mono.defer(() -> collection.upsert(id, doc))
                .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(500)))
                .onErrorResume(e -> {
                    paused = true;
                    connectionLost = true;
                    logger.error("[ERROR] Upsert failed for {}: {}", id, e.getMessage());
                    return Mono.empty();
                });
    }

    private boolean isRetryable(Throwable e) {
        return e instanceof com.couchbase.client.core.error.TimeoutException
                || e instanceof com.couchbase.client.core.error.AmbiguousTimeoutException
                || e instanceof com.couchbase.client.core.error.RequestCanceledException
                || e instanceof MongoTimeoutException
                || (e.getCause() != null && isRetryable(e.getCause()));
    }

    private void sendProgressUpdate(TransferRequest req, int transferred, int total, String status) {
        String migrationKey = req.mongoDatabase() + "." + req.mongoCollection();
        int currentTotal = migrationCurrentTotals.containsKey(migrationKey) ?
                migrationCurrentTotals.get(migrationKey).get() : total;
        Long startTime = migrationStartTimes.get(migrationKey);
        long durationMs = 0L;
        long speed = 0L;
        if (startTime != null && transferred > 0) {
            durationMs = System.currentTimeMillis() - startTime;
            speed = (transferred * 1000) / Math.max(durationMs, 1);
        }
        progressController.sendProgressUpdate(
                req.mongoDatabase(),
                req.mongoCollection(),
                transferred,
                total,
                currentTotal,
                status,
                "MIGRATION",
                0,
                null,
                durationMs,
                speed
        );
        try {
            MigrationProgress prog = new MigrationProgress(
                    req.mongoDatabase(),
                    req.mongoCollection(),
                    transferred,
                    total,
                    currentTotal,
                    status,
                    "MIGRATION",
                    0,
                    null,
                    durationMs,
                    null, speed
            );
            String json = objectMapper.writeValueAsString(prog);
            kafkaTemplate.send(PROGRESS_TOPIC, json);
            logger.info("[INFO] Progress update: {}.{}, {}/{} docs, status={}, duration={}ms, speed={}/s",
                    req.mongoDatabase(), req.mongoCollection(), transferred, currentTotal, status, durationMs, speed);
        } catch (JsonProcessingException e) {
            logger.error("[ERROR] Failed to serialize progress: {}", e.getMessage());
        }
    }

    private void sendCompletionUpdate(TransferRequest req, int transferred, long durationMs, long speed) {
        String migrationKey = req.mongoDatabase() + "." + req.mongoCollection();

        progressController.sendProgressUpdate(
                req.mongoDatabase(),
                req.mongoCollection(),
                transferred,
                transferred,
                transferred,
                "COMPLETED",
                "MIGRATION",
                0,
                null,
                durationMs,
                speed
        );

        try {
            MigrationProgress prog = new MigrationProgress(
                    req.mongoDatabase(),
                    req.mongoCollection(),
                    transferred,
                    transferred,
                    transferred,
                    "COMPLETED",
                    "MIGRATION",
                    0,
                    null,
                    durationMs,
                    null, speed
            );
            String json = objectMapper.writeValueAsString(prog);
            kafkaTemplate.send(PROGRESS_TOPIC, json);
            logger.info("[INFO] Completion update: {}.{}, {} docs, {} ms, {}/s",
                    req.mongoDatabase(), req.mongoCollection(), transferred, durationMs, speed);
        } catch (JsonProcessingException e) {
            logger.error("[ERROR] Failed to serialize completion data: {}", e.getMessage());
        }
    }
}
