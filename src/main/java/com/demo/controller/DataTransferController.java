package com.demo.controller;
 
import com.demo.annotation.Audited;
import com.demo.dto.CouchbaseConnectionDetails;
import com.demo.dto.ErrorResponse;
import com.demo.dto.FunctionTransferRequest;
import com.demo.dto.MongoConnectionDetails;
import com.demo.dto.SingleFunctionTransferRequest;
import com.demo.kafka.KafkaProducer;
import com.demo.security.EncryptionService;
import com.demo.security.SecureConnectionCache;
import com.demo.security.SecureFileUploadService;
import com.demo.service.*;
 
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
 
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus; // CORRECTED IMPORT
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.demo.security.InputValidationService;
 
@RestController
@RequestMapping("/api/transfer")
public class DataTransferController {
    
    private static final Logger logger = LoggerFactory.getLogger(DataTransferController.class);
    
    @Autowired
    private MongoConnectionService mongoConnectionService;
    
    @Autowired
    private CouchbaseConnectionService couchbaseConnectionService;
    
    @Autowired
    private DataTransferService dataTransferService;
    
    @Autowired
    private JwtDecoder jwtDecoder;
    
    @Autowired
    private FunctionTransferService functionTransferService;
    
    @Autowired
    private MongoFunctionService mongoFunctionService;
    
    @Autowired
    private MongoMetadataService mongoMetadataService;
    
    @Autowired
    private KafkaProducer kafkaProducer;
    
    @Autowired
    private SecureConnectionCache connectionCache;
    
    @Autowired
    private SecureFileUploadService fileUploadService;
    
    @Autowired
    private EncryptionService encryptionService;
    
    @Autowired
    private InputValidationService inputValidationService;
    
    private final List<String> kafkaMessages = new ArrayList<>();
    
    @PostMapping("/upload/mongo-certificate")
    @Audited("MONGO_CERT_UPLOAD")
    public ResponseEntity<String> uploadMongoCertificate(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam("certificate")
            @Valid @NotNull
            @RequestPart(value = "certificate") MultipartFile certificate) {
        try {
            fileUploadService.validateAndScanFile(certificate);
            String userId = extractUserIdFromToken(authHeader);
            byte[] encryptedCert = encryptionService.encryptBytes(certificate.getBytes());
            mongoConnectionService.storeCertificate(encryptedCert);
            
            logger.info("MongoDB certificate uploaded successfully for user: {}", userId);
            return ResponseEntity.ok("MongoDB certificate stored successfully");
        } catch (SecurityException e) {
            logger.warn("Security validation failed for certificate upload: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Security validation failed");
        } catch (IOException e) {
            logger.error("Failed to store certificate: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Failed to store certificate");
        } catch (Exception e) {
            logger.error("Unexpected error during certificate upload: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("An error occurred");
        }
    }
    
    @PostMapping("/upload/couchbase-certificate")
    @Audited("COUCHBASE_CERT_UPLOAD")
    public ResponseEntity<String> uploadCouchbaseCertificate(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam("certificate")
            @Valid @NotNull
            @RequestPart(value = "certificate") MultipartFile certificate) {
        try {
            fileUploadService.validateAndScanFile(certificate);
            String userId = extractUserIdFromToken(authHeader);
            byte[] encryptedCert = encryptionService.encryptBytes(certificate.getBytes());
            couchbaseConnectionService.storeCertificate(encryptedCert);
            
            logger.info("Couchbase certificate uploaded successfully for user: {}", userId);
            return ResponseEntity.ok("Couchbase certificate stored successfully");
        } catch (SecurityException e) {
            logger.warn("Security validation failed for certificate upload: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Security validation failed");
        } catch (IOException e) {
            logger.error("Failed to store certificate: {}", e.getMessage());
            return ResponseEntity.badRequest().body("Failed to store certificate");
        } catch (Exception e) {
            logger.error("Unexpected error during certificate upload: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("An error occurred");
        }
    }
    
    @PostMapping("/connect-mongo")
    @Audited("MONGO_CONNECTION")
    public ResponseEntity<String> connectToMongo(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody @Valid MongoConnectionDetails details) {
        try {
            inputValidationService.validateInput(details.getUri(), "MongoDB URI");
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails secureDetails = new MongoConnectionDetails();
            secureDetails.setUri(details.getUri());
            connectionCache.store(userId, "mongo", secureDetails);
            mongoConnectionService.initializeMongoClient(details);
            logger.info("MongoDB connection initialized for user: {}", userId);
            return ResponseEntity.ok("MongoDB connection initialized successfully.");
        } catch (Exception e) {
            logger.error("Failed to connect to MongoDB: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body("Failed to connect to MongoDB");
        }
    }
    
//    @PostMapping("/connect-couchbase")
//    @Audited("COUCHBASE_CONNECTION")
//    public ResponseEntity<String> connectToCouchbase(
//            @RequestHeader("Authorization") String authHeader,
//            @RequestBody @Valid CouchbaseConnectionDetails details) {
//        try {
//            inputValidationService.validateInput(details.getConnectionString(), "Couchbase String");
//            String userId = extractUserIdFromToken(authHeader);
//            if (details.getConnectionString().startsWith("couchbases://") ||
//                    details.getConnectionString().contains("cloud.couchbase.com")) {
//               byte[] certificate = couchbaseConnectionService.getCurrentCertificate();
//               if (certificate != null && certificate.length > 0) {
//                    details.setCertificate(certificate);
//                } else {
//                    logger.warn("Certificate required but not found for user: {}", userId);
//                    return ResponseEntity.badRequest()
//                        .body("Certificate is required for secure Couchbase connections. Please upload a certificate first.");
//                }
//            }
//            connectionCache.store(userId, "couchbase", details);
//            couchbaseConnectionService.initializeWithStoredCertificate(details);
//            logger.info("Couchbase connection initialized for user: {}", userId);
//            return ResponseEntity.ok("Couchbase connection initialized successfully.");
//        } catch (Exception e) {
//            logger.error("Failed to connect to Couchbase: {}", e.getMessage());
//            return ResponseEntity.internalServerError()
//                    .body("Failed to connect to Couchbase");
//        }
//    }
    
    @PostMapping("/select-bucket")
    @Audited("BUCKET_SELECTION")
    public ResponseEntity<?> selectBucket(
            @RequestHeader("Authorization") String authHeader,
            @RequestParam
            @Pattern(regexp = "^[a-zA-Z0-9_-]{1,100}$", message = "Invalid bucket name") String bucketName) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
            if (details == null) {
                logger.warn("No active Couchbase connection for user: {}", userId);
                return ResponseEntity.badRequest().body(Map.of(
                    "status", "ERROR",
                    "message", "No active Couchbase connection"
                ));
            }
            
            details.setBucketName(bucketName);
            connectionCache.store(userId, "couchbase", details);
            couchbaseConnectionService.initializeBucket(bucketName);
            
            Map<String, List<String>> scopeCollections =
                couchbaseConnectionService.listCouchbaseCollections();
            
            logger.info("Bucket {} selected for user: {}", bucketName, userId);
            return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "selectedBucket", bucketName,
                "scopes", scopeCollections
            ));
        } catch (Exception e) {
            logger.error("Failed to select bucket: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "ERROR",
                "message", "Failed to select bucket"
            ));
        }
    }
    
    @GetMapping("/databases")
    @Audited("LIST_DATABASES")
    public ResponseEntity<List<String>> getDatabases(
            @RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
            
            if (details == null) {
                logger.warn("No active MongoDB connection for user: {}", userId);
                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
            }
            
            return ResponseEntity.ok(mongoConnectionService.listDatabases());
        } catch (Exception e) {
            logger.error("Error listing databases: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(List.of("Error listing databases"));
        }
    }
    
    @GetMapping("/databases/{db}/collections")
    @Audited("LIST_COLLECTIONS")
    public ResponseEntity<List<String>> getSourceCollections(
            @RequestHeader("Authorization") String authHeader,
            @PathVariable
            @Pattern(regexp = "^[a-zA-Z0-9_-]{1,64}$", message = "Invalid database name") String db) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
            
            if (details == null) {
                logger.warn("No active MongoDB connection for user: {}", userId);
                return ResponseEntity.badRequest().body(List.of("No active MongoDB connection"));
            }
            
            return ResponseEntity.ok(mongoConnectionService.listCollectionsInDatabase(db));
        } catch (Exception e) {
            logger.error("Error listing collections: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(List.of("Error listing collections"));
        }
    }
    
    @GetMapping("/buckets")
    @Audited("LIST_BUCKETS")
    public ResponseEntity<?> listBuckets(@RequestHeader("Authorization") String authHeader) {
        try {
            return ResponseEntity.ok(Map.of(
                "buckets", couchbaseConnectionService.listAllBuckets()
            ));
        } catch (Exception e) {
            logger.error("Error listing buckets: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to list buckets"
            ));
        }
    }
    
    @GetMapping("/{bucketName}/scopes")
    public ResponseEntity<?> getBucketStructure(
            @RequestHeader("Authorization") String authHeader,
            @PathVariable
            @Pattern(regexp = "^[a-zA-Z0-9_-]{1,100}$", message = "Invalid bucket name") String bucketName) {
        try {
            return ResponseEntity.ok(
                couchbaseConnectionService.getCollectionsForBucket(bucketName)
            );
        } catch (Exception e) {
            logger.error("Error getting bucket structure: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to get bucket structure"
            ));
        }
    }
    
    @GetMapping("/couchbase-collections")
    public ResponseEntity<Map<String, List<String>>> getCouchbaseCollections(
            @RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            CouchbaseConnectionDetails details = connectionCache.getCouchbaseConnection(userId);
            
            if (details == null) {
                logger.warn("No active Couchbase connection for user: {}", userId);
                return ResponseEntity.badRequest().body(Map.of("error", List.of("No active Couchbase connection")));
            }
            
            return ResponseEntity.ok(couchbaseConnectionService.listCouchbaseCollections());
        } catch (Exception e) {
            logger.error("Error listing collections: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of("error", List.of("Error listing collections")));
        }
    }
    
    @PostMapping("/transfer")
    @Audited("DATA_TRANSFER")
    public ResponseEntity<String> transferData(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody DataTransferService.TransferRequest request) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            logger.info("Starting data transfer for user: {}", userId);
            
            dataTransferService.transferCollection(request);
            
            return ResponseEntity.ok("Transfer initiated successfully");
        } catch (Exception e) {
            logger.error("Transfer failed: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                .body("Transfer failed");
        }
    }
    
    @GetMapping("/metadata/mongo")
    @Audited("GET_METADATA")
    public ResponseEntity<?> getMongoMetadata(@RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            MongoConnectionDetails details = connectionCache.getMongoConnection(userId);
            
            if (details == null) {
                logger.warn("No active MongoDB connection for user: {}", userId);
                return ResponseEntity.badRequest().body("No active MongoDB connection");
            }
            
            Map<String, Object> stats = mongoMetadataService.getStorageMetrics();
            kafkaProducer.sendMetadata(stats);
            
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            logger.error("Failed to get metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body("Failed to get metadata");
        }
    }
    
    @KafkaListener(topics = "mongo-metadata", groupId = "spring-boot-consumer")
    public void listen(String message) {
        kafkaMessages.add(message);
    }
    
    @GetMapping("/api/kafka/messages")
    public List<String> getMessages() {
        return kafkaMessages;
    }
    
    @PostMapping("/transfer-functions")
    @Audited("FUNCTION_TRANSFER")
    public ResponseEntity<?> transferFunctions(@RequestBody FunctionTransferRequest request) {
        try {
            functionTransferService.transferAllFunctions(request);
            return ResponseEntity.ok("Functions transferred successfully");
        } catch (Exception e) {
            logger.error("Function transfer failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
        }
    }
    
    @PostMapping("/transfer-function")
    @Audited("SINGLE_FUNCTION_TRANSFER")
    public ResponseEntity<?> transferSingleFunction(@RequestBody SingleFunctionTransferRequest request) {
        try {
            boolean success = functionTransferService.transferSingleFunction(
                request.getMongoDatabase(),
                request.getFunctionName(),
                request.getCouchbaseScope()
            );
            
            if (success) {
                logger.info("Function '{}' transferred successfully", request.getFunctionName());
                return ResponseEntity.ok("Function '" + request.getFunctionName() + "' transferred successfully");
            } else {
                logger.warn("Function '{}' not found in database {}",
                    request.getFunctionName(), request.getMongoDatabase());
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse("Function not found",
                        "Function '" + request.getFunctionName() + "' not found in database " +
                        request.getMongoDatabase(), null));
            }
        } catch (Exception e) {
            logger.error("Function transfer failed: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Function transfer failed", e.getMessage(), null));
        }
    }
    
    @GetMapping("/mongo-functions")
    public ResponseEntity<?> getMongoFunctions(
            @RequestParam String database,
            @RequestParam(required = false, defaultValue = "false") boolean includeSystem) {
        try {
            List<org.bson.Document> functions = mongoFunctionService.getAllFunctions(database, includeSystem);
            return ResponseEntity.ok(functions);
        } catch (Exception e) {
            logger.error("Error retrieving functions: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(new ErrorResponse("Error retrieving functions", e.getMessage(), database));
        }
    }
    
    private String extractUserIdFromToken(String authHeader) {
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new SecurityException("Invalid authorization header");
        }
        String token = authHeader.substring(7);
        Jwt jwt = jwtDecoder.decode(token);
        return jwt.getClaim("sub");
    }
    
    // New API to stop ongoing migration and CDC
    @PostMapping("/stop")
    @Audited("STOP_MIGRATION")
    public ResponseEntity<String> stopMigration(
            @RequestHeader("Authorization") String authHeader) {
        try {
            String userId = extractUserIdFromToken(authHeader);
            logger.info("Stop migration requested by user: {}", userId);
            dataTransferService.stopMigration();
            return ResponseEntity.ok("Migration and CDC stop requested successfully.");
        } catch (Exception e) {
            logger.error("Failed to stop migration: {}", e.getMessage());
            return ResponseEntity.internalServerError()
                    .body("Failed to stop migration");
        }
    }
    
    @PostMapping("/connect-couchbase")
    @Audited("COUCHBASE_CONNECTION")
    public ResponseEntity<String> connectToCouchbase(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody @Valid CouchbaseConnectionDetails details) {
        try {
            inputValidationService.validateInput(details.getConnectionString(), "Couchbase Connection String");
            String userId = extractUserIdFromToken(authHeader);
            if (!details.isCloudConnection() && !details.isOnPremConnection()) {
                return ResponseEntity.badRequest()
                    .body("Invalid connection string. Must start with 'couchbase://' for on-prem or 'couchbases://' for Capella.");
            }
            if (details.isCloudConnection()) {
                byte[] certificate = couchbaseConnectionService.getCurrentCertificate();
                if (certificate == null || certificate.length == 0) {
                    logger.warn("Certificate required for Couchbase Capella but not found for user: {}", userId);
                    return ResponseEntity.badRequest()
                        .body("Certificate is required for Couchbase Capella connections. Please upload a certificate first.");
                }
                details.setCertificate(certificate);
                logger.info("Connecting to Couchbase Capella with certificate for user: {}", userId);
            } else {
                logger.info("Connecting to on-prem Couchbase for user: {}", userId);
            }
            
            connectionCache.store(userId, "couchbase", details);
            if (details.isCloudConnection()) {
                couchbaseConnectionService.initializeWithStoredCertificate(details);
            } else {
                couchbaseConnectionService.initializeConnection(details);
            }
            
            logger.info("Couchbase connection initialized successfully for user: {}", userId);
            return ResponseEntity.ok("Couchbase connection initialized successfully.");
            
        } catch (Exception e) {
            logger.error("Failed to connect to Couchbase: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body("Failed to connect to Couchbase: " + e.getMessage());
        }
    }
 
}
 