package com.demo.service;
 
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.query.QueryResult;
import org.springframework.stereotype.Service;
 
@Service
public class CouchbaseFunctionService {
    private final CouchbaseConnectionService connectionService;
 
    public CouchbaseFunctionService(CouchbaseConnectionService connectionService) {
        this.connectionService = connectionService;
    }
 
    public void createFunction(String n1qlQuery) {
        Cluster cluster = connectionService.getCluster();
        try {
            System.out.println("Executing N1QL Function Creation:");
            System.out.println(n1qlQuery);
            
            QueryResult result = cluster.query(n1qlQuery);
            
            if (result.metaData().warnings().isEmpty()) {
                System.out.println("Function created successfully");
            } else {
                System.out.println("Function created with warnings: " +
                    result.metaData().warnings());
            }
        } catch (CouchbaseException e) {
            System.err.println("Failed to create function:");
            System.err.println("Query: " + n1qlQuery);
            throw new RuntimeException("Couchbase function creation failed: " + e.getMessage(), e);
        }
    }
 
    public String getBucketName() {
        return connectionService.getBucket().name();
    }
}
 