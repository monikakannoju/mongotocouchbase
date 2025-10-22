package com.demo.service;
 
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
 
import java.util.*;
import java.util.regex.Pattern;
 
@Service
public class MongoFunctionService {
    private static final Logger logger = LoggerFactory.getLogger(MongoFunctionService.class);
 
    private final MongoConnectionService mongoConnectionService;
 
    public MongoFunctionService(MongoConnectionService mongoConnectionService) {
        this.mongoConnectionService = mongoConnectionService;
    }
 
    public Document getFunctionByName(String databaseName, String functionName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }
        if (functionName == null || functionName.trim().isEmpty()) {
            throw new IllegalArgumentException("Function name cannot be null or empty");
        }
 
        try {
            MongoClient mongoClient = mongoConnectionService.getMongoClient();
            MongoDatabase database = mongoClient.getDatabase(databaseName);
 
            if (!database.listCollectionNames().into(new ArrayList<>()).contains("system.js")) {
                logger.warn("system.js collection not found in database {}", databaseName);
                return null;
            }
 
            MongoCollection<Document> functionsCollection = database.getCollection("system.js");
            Document query = new Document("_id", functionName);
            Document functionDoc = functionsCollection.find(query).first();
 
            if (functionDoc == null) {
                logger.warn("Function '{}' not found in database {}", functionName, databaseName);
                return null;
            }
 
            logger.info("Retrieved function '{}' from database {}", functionName, databaseName);
            return functionDoc;
 
        } catch (Exception e) {
            logger.error("Failed to retrieve function '{}' from database {}", functionName, databaseName, e);
            throw new RuntimeException("Error retrieving MongoDB function", e);
        }
    }
 
    public Document getFunctionByPattern(String databaseName, String functionName, String functionType) {
        List<Document> functions = getFunctionsByPattern(databaseName, functionType);
        return functions.stream()
                .filter(f -> functionName.equals(f.getString("_id")))
                .findFirst()
                .orElse(null);
    }
 
    public List<Document> getFunctionsByPattern(String databaseName, String functionType) {
        List<Document> allFunctions = getAllFunctions(databaseName, false);
        List<Document> filteredFunctions = new ArrayList<>();
 
        Pattern pattern = getPatternForFunctionType(functionType);
 
        for (Document functionDoc : allFunctions) {
            Object value = functionDoc.get("value");
            String functionCode = value != null ? value.toString() : "";
            if (pattern.matcher(functionCode).find()) {
                filteredFunctions.add(functionDoc);
            }
        }
 
        return filteredFunctions;
    }
 
    // NEW: Method to get array processing functions
    public List<Document> getArrayProcessingFunctions(String databaseName) {
        List<Document> allFunctions = getAllFunctions(databaseName, false);
        List<Document> arrayFunctions = new ArrayList<>();
 
        // Pattern to match common array operations
        Pattern arrayPattern = Pattern.compile(
            "\\$(map|filter|reduce|in|all|size|slice|concatArrays|arrayElemAt|first|last|sortArray)\\b" +
            "|Array\\.(map|filter|reduce|includes|find|findIndex|some|every|sort|slice|concat|join|push|pop|shift|unshift)" +
            "|\\.(map|filter|reduce|includes|find|findIndex|some|every|sort|slice|concat|join|push|pop|shift|unshift)\\(",
            Pattern.CASE_INSENSITIVE
        );
 
        for (Document functionDoc : allFunctions) {
            Object value = functionDoc.get("value");
            String functionCode = value != null ? value.toString() : "";
            if (arrayPattern.matcher(functionCode).find()) {
                arrayFunctions.add(functionDoc);
            }
        }
 
        return arrayFunctions;
    }
 
    private Pattern getPatternForFunctionType(String functionType) {
        switch (functionType.toLowerCase()) {
            case "shell":
                return Pattern.compile("db\\.eval|load\\(");
            case "application":
                return Pattern.compile("MongoClient|MongoCollection|MongoDatabase");
            case "atlas":
                return Pattern.compile("context\\.functions|context\\.user");
            case "trigger":
                return Pattern.compile("changeEvent\\.|operationType");
            case "realm":
                return Pattern.compile("realmFunction|sync\\(");
            case "script":
                return Pattern.compile("load\\(");
            case "driver":
                return Pattern.compile("MongoClient|MongoCollection|MongoDatabase");
            case "aggregation":
                return Pattern.compile("\\$function|\\$let|\\$cond|aggregate\\(");
            case "mapreduce":
                return Pattern.compile("mapReduce\\(|mapFunction|reduceFunction");
            case "where":
                return Pattern.compile("\\$where|this\\.");
            case "eval":
                return Pattern.compile("db\\.eval\\(");
            case "expression":
                return Pattern.compile("\\$function|\\$let|\\$cond|\\$expr");
            case "array": // NEW: Pattern for array processing functions
                return Pattern.compile(
                    "\\$(map|filter|reduce|in|all|size|slice|concatArrays|arrayElemAt|first|last|sortArray)\\b" +
                    "|Array\\.(map|filter|reduce|includes|find|findIndex|some|every|sort|slice|concat|join|push|pop|shift|unshift)" +
                    "|\\.(map|filter|reduce|includes|find|findIndex|some|every|sort|slice|concat|join|push|pop|shift|unshift)\\(",
                    Pattern.CASE_INSENSITIVE
                );
            case "versioned":
                return Pattern.compile(".*"); // Match all (pattern not used)
            default:
                return Pattern.compile(""); // Match nothing for unknown types
        }
    }
 
    public List<Document> getAllFunctions(String databaseName, boolean includeSystemFunctions) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }
 
        try {
            MongoClient mongoClient = mongoConnectionService.getMongoClient();
            MongoDatabase database = mongoClient.getDatabase(databaseName);
 
            List<String> collections = database.listCollectionNames().into(new ArrayList<>());
            logger.debug("Available collections in {}: {}", databaseName, collections);
 
            if (!collections.contains("system.js")) {
                logger.warn("system.js collection not found in database {}", databaseName);
                return Collections.emptyList();
            }
 
            MongoCollection<Document> functionsCollection = database.getCollection("system.js");
            List<Document> functions = new ArrayList<>();
 
            try (MongoCursor<Document> cursor = functionsCollection.find().iterator()) {
                while (cursor.hasNext()) {
                    Document functionDoc = cursor.next();
                    logger.debug("Processing function document: {}", functionDoc.toJson());
 
                    String functionId = functionDoc.getString("_id");
                    if (functionId != null) {
                        if (includeSystemFunctions || !functionId.startsWith("system.")) {
                            functions.add(functionDoc);
                        }
                    } else {
                        logger.warn("Found function document without _id: {}", functionDoc.toJson());
                    }
                }
            }
 
            logger.info("Retrieved {} functions from database {}", functions.size(), databaseName);
            return functions;
 
        } catch (Exception e) {
            logger.error("Failed to retrieve functions from database {}", databaseName, e);
            throw new RuntimeException("Error retrieving MongoDB functions", e);
        }
    }
 
    public List<Document> getSystemJSFunctions(String databaseName) {
        return getAllFunctions(databaseName, false);
    }
 
    public List<Document> getAggregationFunctions(String databaseName) {
        return getFunctionsByPattern(databaseName, "aggregation");
    }
 
    public List<Document> getMapReduceFunctions(String databaseName) {
        return getFunctionsByPattern(databaseName, "mapreduce");
    }
 
    public List<Document> getWhereQueryFunctions(String databaseName) {
        return getFunctionsByPattern(databaseName, "where");
    }
 
    public List<Document> getEvalFunctions(String databaseName) {
        return getFunctionsByPattern(databaseName, "eval");
    }
 
    public List<Document> getAggregationExpressions(String databaseName) {
        return getFunctionsByPattern(databaseName, "expression");
    }
 
    public List<Document> getVersionedFunctions(String databaseName) {
        if (databaseName == null || databaseName.trim().isEmpty()) {
            throw new IllegalArgumentException("Database name cannot be null or empty");
        }
        try {
            MongoClient mongoClient = mongoConnectionService.getMongoClient();
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            List<String> collections = database.listCollectionNames().into(new ArrayList<>());
            if (!collections.contains("function_registry")) {
                logger.warn("function_registry collection not found in database {}", databaseName);
                return Collections.emptyList();
            }
            MongoCollection<Document> registryCollection = database.getCollection("function_registry");
            List<Document> funcs = registryCollection.find().into(new ArrayList<>());
            logger.info("Retrieved {} versioned functions from function_registry in database {}", funcs.size(), databaseName);
            return funcs;
 
        } catch (Exception e) {
            logger.error("Failed to retrieve versioned functions from database {}", databaseName, e);
            throw new RuntimeException("Error retrieving MongoDB versioned functions", e);
        }
    }
}
 