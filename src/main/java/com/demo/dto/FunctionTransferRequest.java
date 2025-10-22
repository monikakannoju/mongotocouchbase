 
package com.demo.dto;
 
import java.util.ArrayList;

import java.util.List;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
 
public class FunctionTransferRequest {
	@NotBlank(message = "MongoDB database is required")
    @Pattern(regexp = "^[a-zA-Z0-9_-]{1,64}$", 
             message = "Invalid database name")
    private String mongoDatabase;
    
    @Pattern(regexp = "^[a-zA-Z0-9_-]{0,100}$", 
             message = "Invalid bucket name")
    private String couchbaseBucket;
    
    @Pattern(regexp = "^[a-zA-Z0-9_-]{0,100}$", 
             message = "Invalid scope name")
    private String couchbaseScope;
    
    private boolean includeSystemFunctions;
    
    @Size(max = 100, message = "Too many function names (max 100)")
    private List<@Pattern(regexp = "^[a-zA-Z0-9_.-]{1,255}$") String> functionNames = new ArrayList<>();
    
    private boolean continueOnError = false;
    
    @Pattern(regexp = "^[a-zA-Z0-9_-]{0,100}$", 
             message = "Invalid checkpoint ID")
    private String checkpointId;
    
    private boolean resumeFromCheckpoint;
    
    @Min(value = 0, message = "Max retries cannot be negative")
    @Max(value = 10, message = "Max retries cannot exceed 10")
    private int maxRetries = 3;
    
    @Min(value = 100, message = "Retry delay must be at least 100ms")
    @Max(value = 60000, message = "Retry delay cannot exceed 60 seconds")
    private int retryDelayMs = 1000;
    
    @Min(value = 1000, message = "Function timeout must be at least 1 second")
    @Max(value = 300000, message = "Function timeout cannot exceed 5 minutes")
    private int functionTimeoutMs = 5000;
    public String getMongoDatabase() {
        return mongoDatabase;
    }
 
    public void setMongoDatabase(String mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }
 
    public String getCouchbaseBucket() {
        return couchbaseBucket;
    }
 
    public void setCouchbaseBucket(String couchbaseBucket) {
        this.couchbaseBucket = couchbaseBucket;
    }
 
    public String getCouchbaseScope() {
        return couchbaseScope;
    }
 
    public void setCouchbaseScope(String couchbaseScope) {
        this.couchbaseScope = couchbaseScope;
    }
 
    public boolean isIncludeSystemFunctions() {
        return includeSystemFunctions;
    }
 
    public void setIncludeSystemFunctions(boolean includeSystemFunctions) {
        this.includeSystemFunctions = includeSystemFunctions;
    }
 
    public List<String> getFunctionNames() {
        return functionNames;
    }
 
    public void setFunctionNames(List<String> functionNames) {
        this.functionNames = functionNames != null ? functionNames : new ArrayList<>();
    }
 
    public boolean isContinueOnError() {
        return continueOnError;
    }
 
    public void setContinueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }
}
 