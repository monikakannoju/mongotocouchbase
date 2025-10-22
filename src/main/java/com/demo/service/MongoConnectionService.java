package com.demo.service;

import com.demo.dto.MongoConnectionDetails;
import com.demo.security.EncryptionService;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import org.apache.http.ssl.SSLContexts;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.security.cert.Certificate;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.SSLContext;

@Service
public class MongoConnectionService {
    
    @Autowired
    private EncryptionService encryptionService;
    
    private MongoClient mongoClient;
    private byte[] storedCertificate;
    
    public void storeCertificate(byte[] encryptedCertificate) {
        this.storedCertificate = encryptedCertificate;
    }

    public void initializeWithStoredCertificate(MongoConnectionDetails details) throws Exception {
        if (storedCertificate == null) {
            throw new IllegalStateException("No certificate stored");
        }
        byte[] decryptedCert = encryptionService.decryptBytes(storedCertificate);
        initializeWithCertificate(details.getUri(), decryptedCert);
    }

    public void initializeMongoClient(MongoConnectionDetails details) {
        String uri = details.getUri();
        if (uri != null && !uri.isEmpty()) {
            if (encryptionService.isEncrypted(uri)) {
                uri = encryptionService.decryptSensitive(uri);
            }
            this.mongoClient = MongoClients.create(uri);
        } else {
            String username = details.getUsername();
            String password = details.getPassword();
            if (username != null && encryptionService.isEncrypted(username)) {
                username = encryptionService.decryptSensitive(username);
            }
            if (password != null && encryptionService.isEncrypted(password)) {
                password = encryptionService.decryptSensitive(password);
            }
            
            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                    builder.hosts(List.of(new ServerAddress(details.getHost(), details.getPort())))
                );

            if (username != null && password != null) {
                MongoCredential credential = MongoCredential.createCredential(
                    username,
                    details.getDatabase(),
                    password.toCharArray()
                );
                settingsBuilder.credential(credential);
            }

            MongoClientSettings settings = settingsBuilder.build();
            this.mongoClient = MongoClients.create(settings);
        }
    }
    
    public void initializeWithCertificate(String connectionString, byte[] certificate) throws Exception {

        if (encryptionService.isEncrypted(connectionString)) {
            connectionString = encryptionService.decryptSensitive(connectionString);
        }

        KeyStore trustStore = KeyStore.getInstance("JKS");
        trustStore.load(null, null);
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate cert = cf.generateCertificate(new ByteArrayInputStream(certificate));
        trustStore.setCertificateEntry("mongo-atlas", cert);

        SSLContext sslContext = SSLContexts.custom()
            .loadTrustMaterial(trustStore, null)
            .build();

        this.mongoClient = MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(connectionString))
                .applyToSslSettings(builder ->
                    builder.enabled(true)
                    .context(sslContext)
                )
                .build()
        );
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public List<String> listDatabases() {
        List<String> dbNames = new ArrayList<>();
        if (mongoClient != null) {
            mongoClient.listDatabaseNames().forEach(dbNames::add);
        }
        return dbNames;
    }

    public List<String> listCollectionsInDatabase(String dbName) {
        if (mongoClient != null) {
            return mongoClient.getDatabase(dbName).listCollectionNames().into(new ArrayList<>());
        }
        return new ArrayList<>();
    }
  
    public void closeConnection() {
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
  
    public void clearStoredCertificate() {
        if (storedCertificate != null) {
            for (int i = 0; i < storedCertificate.length; i++) {
                storedCertificate[i] = 0;
            }
            storedCertificate = null;
        }
    }
}