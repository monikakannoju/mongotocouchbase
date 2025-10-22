package com.demo.service;

import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.demo.dto.CouchbaseConnectionDetails;
import com.demo.security.EncryptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManagerFactory;

@Service
public class CouchbaseConnectionService {
    
    private static final Logger logger = LoggerFactory.getLogger(CouchbaseConnectionService.class);
    
    @Autowired
    private EncryptionService encryptionService;
    
    private Cluster cluster;
    private Bucket bucket;
    private byte[] currentCertificate;
    private CouchbaseConnectionDetails currentDetails;
    
    // Certificate management
    public byte[] getCurrentCertificate() {
        return currentCertificate;
    }
    
    public void storeCertificate(byte[] encryptedCertificate) {
        this.currentCertificate = encryptedCertificate;
        logger.info("Certificate stored successfully");
    }
    
    public void clearStoredCertificate() {
        if (currentCertificate != null) {
            // Securely wipe the byte array
            for (int i = 0; i < currentCertificate.length; i++) {
                currentCertificate[i] = 0;
            }
            currentCertificate = null;
        }
        logger.info("Certificate cleared");
    }
    
    // Connection initialization methods
    
    public void initializeWithStoredCertificate(CouchbaseConnectionDetails details) throws Exception {
        if (currentCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        byte[] decryptedCert = encryptionService.decryptBytes(currentCertificate);
        initializeWithCertificate(details, decryptedCert);
    }
    
    public void initializeWithCertificate(CouchbaseConnectionDetails details, byte[] certificate) throws Exception {
        this.currentDetails = details;
        
        if (!details.isCloudConnection()) {
            throw new IllegalArgumentException("Certificate provided but connection string is not for Couchbase Capella");
        }
        
        logger.info("Initializing Couchbase Capella connection with certificate");
        
        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        Certificate cert = CertificateFactory.getInstance("X.509")
            .generateCertificate(new ByteArrayInputStream(certificate));
        trustStore.setCertificateEntry("couchbase-cert", cert);

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        ClusterEnvironment env = ClusterEnvironment.builder()
            .securityConfig(SecurityConfig.enableTls(true)
                .trustManagerFactory(tmf)
                .enableHostnameVerification(true)) // Enable for cloud/Capella
            .timeoutConfig(TimeoutConfig
                .kvTimeout(Duration.ofSeconds(15))
                .connectTimeout(Duration.ofSeconds(30))
                .queryTimeout(Duration.ofSeconds(75))
                .disconnectTimeout(Duration.ofSeconds(10)))
            .ioConfig(IoConfig.enableDnsSrv(true)) // Enable DNS SRV for cloud
            .build();

        String username = decryptIfEncrypted(details.getUsername());
        String password = decryptIfEncrypted(details.getPassword());
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(username, password)
                .environment(env)
        );

        if (details.getBucketName() != null && !details.getBucketName().isEmpty()) {
            this.bucket = cluster.bucket(details.getBucketName());
            this.bucket.waitUntilReady(Duration.ofSeconds(30));
        }
        
        logger.info("Successfully connected to Couchbase Capella: {}", details.getConnectionString());
    }

    public void initializeConnection(CouchbaseConnectionDetails details) throws Exception {
        this.currentDetails = details;
        
        if (details.isCloudConnection()) {
            throw new IllegalArgumentException("Cloud connection detected. Use certificate-based initialization.");
        }
        
        logger.info("Initializing on-prem Couchbase connection");
        
        ClusterEnvironment env = ClusterEnvironment.builder()
            .timeoutConfig(TimeoutConfig
                .kvTimeout(Duration.ofSeconds(15))
                .connectTimeout(Duration.ofSeconds(30))
                .queryTimeout(Duration.ofSeconds(75))
                .disconnectTimeout(Duration.ofSeconds(10)))
            .ioConfig(IoConfig.enableDnsSrv(false)) // Disable DNS SRV for on-prem
            .build();

        String username = decryptIfEncrypted(details.getUsername());
        String password = decryptIfEncrypted(details.getPassword());
        
        this.cluster = Cluster.connect(
            details.getConnectionString(),
            ClusterOptions.clusterOptions(username, password)
                .environment(env)
        );

        if (details.getBucketName() != null && !details.getBucketName().isEmpty()) {
            this.bucket = cluster.bucket(details.getBucketName());
            this.bucket.waitUntilReady(Duration.ofSeconds(30));
        }
        
        logger.info("Successfully connected to on-prem Couchbase: {}", details.getConnectionString());
    }

    public void initializeBucket(String bucketName) {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        this.bucket = cluster.bucket(bucketName);
        this.bucket.waitUntilReady(Duration.ofSeconds(30));
        logger.info("Bucket initialized: {}", bucketName);
    }

    // Bucket and collection operations
    
    public List<String> listAllBuckets() {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        List<String> buckets = cluster.buckets().getAllBuckets().values().stream()
            .map(BucketSettings::name)
            .collect(Collectors.toList());
        logger.info("Found {} buckets", buckets.size());
        return buckets;
    }

    public Map<String, List<String>> listCouchbaseCollections() {
        if (bucket == null) {
            throw new IllegalStateException("Bucket not initialized");
        }
        Map<String, List<String>> scopeCollections = new HashMap<>();
        List<ScopeSpec> scopes = bucket.collections().getAllScopes();
        
        for (ScopeSpec scope : scopes) {
            List<String> collectionNames = scope.collections().stream()
                .map(CollectionSpec::name)
                .collect(Collectors.toList());
            scopeCollections.put(scope.name(), collectionNames);
        }
        
        logger.info("Found {} scopes with collections", scopeCollections.size());
        return scopeCollections;
    }

    public Map<String, List<String>> getCollectionsForBucket(String bucketName) {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        
        Bucket tempBucket = cluster.bucket(bucketName);
        tempBucket.waitUntilReady(Duration.ofSeconds(10));
        
        Map<String, List<String>> scopeCollections = new HashMap<>();
        List<ScopeSpec> scopes = tempBucket.collections().getAllScopes();
        
        for (ScopeSpec scope : scopes) {
            List<String> collectionNames = scope.collections().stream()
                .map(CollectionSpec::name)
                .collect(Collectors.toList());
            scopeCollections.put(scope.name(), collectionNames);
        }
        
        logger.info("Found {} scopes in bucket {}", scopeCollections.size(), bucketName);
        return scopeCollections;
    }

    public Collection getTargetCollection(String bucketName, String scopeName, String collectionName) {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        
        Bucket targetBucket = cluster.bucket(bucketName);
        targetBucket.waitUntilReady(Duration.ofSeconds(10));
        
        Collection collection = targetBucket.scope(scopeName).collection(collectionName);
        logger.info("Target collection resolved: {}.{}.{}", bucketName, scopeName, collectionName);
        
        return collection;
    }

    // Connection status and info
    
    public Bucket getBucket() {
        return bucket;
    }

    public Cluster getCluster() {
        return cluster;
    }
    
    public void ping() {
        if (this.cluster == null) {
            throw new IllegalStateException("Couchbase cluster is not initialized.");
        }
        try {
            this.cluster.ping();
            logger.info("Cluster ping successful");
        } catch (Exception ex) {
            throw new RuntimeException("Couchbase ping failed: " + ex.getMessage(), ex);
        }
    }
    
    public String getNodes() {
        if (cluster == null) {
            throw new IllegalStateException("Couchbase cluster is not initialized");
        }
        return currentDetails.getConnectionString();
    }
    
    public String getUsername() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getUsername();
    }
    
    public String getPassword() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getPassword();
    }
    
    public String getBucketName() {
        if (currentDetails == null) {
            throw new IllegalStateException("No active Couchbase connection");
        }
        return currentDetails.getBucketName();
    }
    
    public void closeConnection() {
        if (cluster != null) {
            try {
                cluster.disconnect();
                logger.info("Couchbase connection closed successfully");
            } catch (Exception e) {
                logger.warn("Error closing Couchbase connection: {}", e.getMessage());
            }
            cluster = null;
        }
        bucket = null;
        currentDetails = null;
    }
    
    // Utility methods
    
    private String decryptIfEncrypted(String value) {
        if (value != null && encryptionService != null && encryptionService.isEncrypted(value)) {
            return encryptionService.decryptSensitive(value);
        }
        return value;
    }
    
    public boolean isConnected() {
        try {
            if (cluster != null) {
                cluster.ping();
                return true;
            }
            return false;
        } catch (Exception e) {
            logger.warn("Connection check failed: {}", e.getMessage());
            return false;
        }
    }
   
    public Map<String, Object> getClusterInfo() {
        if (cluster == null) {
            throw new IllegalStateException("Cluster not initialized");
        }
        
        Map<String, Object> info = new HashMap<>();
        info.put("connected", isConnected());
        info.put("bucketCount", listAllBuckets().size());
        info.put("connectionString", currentDetails != null ? currentDetails.getConnectionString() : "N/A");
        info.put("username", currentDetails != null ? currentDetails.getUsername() : "N/A");
        info.put("connectionType", currentDetails != null && currentDetails.isCloudConnection() ? "Capella" : "On-prem");
        
        return info;
    }
    
    public boolean testBasicConnectivity(String connectionString) {
        try {
            // Extract host and port from connection string
            String cleanString = connectionString.replace("couchbase://", "").replace("couchbases://", "");
            String[] parts = cleanString.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8091;
            
            logger.info("Testing connectivity to {}:{}", host, port);
            
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 10000);
                logger.info("Basic connectivity test successful to {}:{}", host, port);
                return true;
            }
        } catch (Exception e) {
            logger.warn("Basic connectivity test failed: {}", e.getMessage());
            return false;
        }
    }
    
    public Map<String, Object> testConnection(CouchbaseConnectionDetails details) {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Test basic connectivity first
            boolean basicConnectivity = testBasicConnectivity(details.getConnectionString());
            result.put("basicConnectivity", basicConnectivity);
            
            if (!basicConnectivity) {
                result.put("status", "ERROR");
                result.put("message", "Cannot reach Couchbase server at " + details.getConnectionString());
                return result;
            }
            
            // Try to initialize connection based on type
            if (details.isCloudConnection()) {
                if (details.getCertificate() != null && details.getCertificate().length > 0) {
                    initializeWithCertificate(details, details.getCertificate());
                } else {
                    byte[] cert = getCurrentCertificate();
                    if (cert != null && cert.length > 0) {
                        initializeWithStoredCertificate(details);
                    } else {
                        throw new IllegalStateException("Certificate required for Capella connection");
                    }
                }
            } else {
                initializeConnection(details);
            }
            
            // Test if actually connected
            boolean isConnected = isConnected();
            result.put("status", "SUCCESS");
            result.put("connected", isConnected);
            result.put("message", "Connection established successfully");
            result.put("connectionType", details.isCloudConnection() ? "Capella" : "On-prem");
            
            // Add cluster info if connected
            if (isConnected) {
                result.put("clusterInfo", getClusterInfo());
            }
            
        } catch (Exception e) {
            result.put("status", "ERROR");
            result.put("connected", false);
            result.put("message", e.getMessage());
            result.put("errorType", e.getClass().getSimpleName());
            result.put("stackTrace", Arrays.toString(e.getStackTrace()));
        } finally {
            // Always close connection after test
            closeConnection();
        }
        
        return result;
    }
    
    // Helper method to detect connection type from connection string
    public static boolean isCloudConnection(String connectionString) {
        return connectionString != null && 
               (connectionString.startsWith("couchbases://") ||
                connectionString.contains("cloud.couchbase.com") ||
                connectionString.contains(".cloud.couchbase.com"));
    }
}
//on prem/cloud 