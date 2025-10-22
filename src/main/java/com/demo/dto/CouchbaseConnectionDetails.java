package com.demo.dto;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
public class CouchbaseConnectionDetails {
	@NotBlank(message = "Connection string is required")
    @Pattern(regexp =  "^couchbases?://.*$",
    //"^couchbases?://[a-zA-Z0-9.-]+(:[0-9]+)?(/.*)?$", 
             message = "Invalid connection string format")
    @Size(max = 255, message = "Connection string too long")
    private String connectionString;
    @NotBlank(message = "Username is required")
    @Pattern(regexp = "^[a-zA-Z0-9_-]{1,100}$", 
             message = "Invalid username format")
    private String username;
    @NotBlank(message = "Password is required")
    @Size(min = 8, max = 128, message = "Password must be between 8 and 128 characters")
    private String password;
    @Pattern(regexp = "^[a-zA-Z0-9_-]{0,100}$", 
             message = "Invalid bucket name format")
    private String bucketName;
 
    @Size(max = 10485760, message = "Certificate too large")
 
	private byte[] certificate;
    public byte[] getCertificate() {
		return certificate;
	}
 
	public void setCertificate(byte[] certificate) {
		this.certificate = certificate;
	}
 
	public String getConnectionString() {
        return connectionString;
    }
 
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }
 
    public String getUsername() {
        return username;
    }
 
    public void setUsername(String username) {
        this.username = username;
    }
 
    public String getPassword() {
        return password;
    }
 
    public void setPassword(String password) {
        this.password = password;
    }
 
    public String getBucketName() {
        return bucketName;
    }
 
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
    public boolean isCloudConnection() {
        return this.connectionString != null && 
               (this.connectionString.startsWith("couchbases://") ||
                this.connectionString.contains("cloud.couchbase.com") ||
                this.connectionString.contains(".cloud.couchbase.com"));
    }
    public boolean isOnPremConnection() {
        return this.connectionString != null && 
               this.connectionString.startsWith("couchbase://");
    }
}