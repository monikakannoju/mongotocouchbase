 package com.demo.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class SingleFunctionTransferRequest {
	@NotBlank(message = "MongoDB database is required")
    @Pattern(regexp = "^[a-zA-Z0-9_-]{1,64}$", 
             message = "Invalid database name")
    private String mongoDatabase;
    
    @NotBlank(message = "Function name is required")
    @Pattern(regexp = "^[a-zA-Z0-9_.-]{1,255}$", 
             message = "Invalid function name")
    private String functionName;
    
    @Pattern(regexp = "^[a-zA-Z0-9_-]{0,100}$", 
             message = "Invalid scope name")
    private String couchbaseScope;
 
    public String getMongoDatabase() {
        return mongoDatabase;
    }
 
    public void setMongoDatabase(String mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }
 
    public String getFunctionName() {
        return functionName;
    }
 
    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }
 
    public String getCouchbaseScope() {
        return couchbaseScope;
    }
 
    public void setCouchbaseScope(String couchbaseScope) {
        this.couchbaseScope = couchbaseScope;
    }
}
 