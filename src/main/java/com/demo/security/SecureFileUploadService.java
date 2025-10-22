package com.demo.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.nio.file.Paths;
import java.security.cert.CertificateFactory;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class SecureFileUploadService {
    
    private static final Logger logger = LoggerFactory.getLogger(SecureFileUploadService.class);
    private static final Set<String> ALLOWED_EXTENSIONS = Set.of(
        ".pem", 
        ".crt", 
        ".cer", 
        ".key", 
        ".txt"  
    );
    
    @Value("${security.file-upload.max-size:5242880}") 
    private long maxFileSize;
    
    public void validateAndScanFile(MultipartFile file) throws SecurityException {
        if (file == null || file.isEmpty()) {
            throw new SecurityException("File is empty or null");
        }
        
        if (file.getSize() > maxFileSize) {
            throw new SecurityException("File exceeds maximum size of " + maxFileSize + " bytes");
        }
        
        String filename = file.getOriginalFilename();
        if (filename == null || filename.isEmpty()) {
            throw new SecurityException("Invalid filename");
        }
        
        filename = Paths.get(filename).getFileName().toString();
        
        if (filename.contains("\0")) {
            throw new SecurityException("Invalid filename - contains null bytes");
        }
        
        if (!hasAllowedExtension(filename)) {
            throw new SecurityException("Invalid file type. Allowed extensions: " + ALLOWED_EXTENSIONS);
        }
        
        validateCertificateContent(file);
        
        logger.info("File validation successful for: {}", filename);
    }
    
    private boolean hasAllowedExtension(String filename) {
        if (filename == null) {
            return false;
        }
        
        String lowerFilename = filename.toLowerCase();
        return ALLOWED_EXTENSIONS.stream()
            .anyMatch(ext -> lowerFilename.endsWith(ext));
    }
    
    private void validateCertificateContent(MultipartFile file) throws SecurityException {
        try {
            byte[] content = file.getBytes();
            String contentStr = new String(content);
            String filename = file.getOriginalFilename();
            if (filename != null && filename.toLowerCase().endsWith(".txt")) {
                validateTextCertificate(contentStr, filename);
                return;
            }
            if (contentStr.contains("-----BEGIN") && contentStr.contains("-----END")) {
                return;
            }
            
            try {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                cf.generateCertificate(new ByteArrayInputStream(content));
                return; 
            } catch (Exception certEx) {
                if (filename != null && filename.toLowerCase().endsWith(".key")) {
                    if (contentStr.contains("-----BEGIN") && contentStr.contains("KEY-----")) {
                        return; 
                    }
                    throw new SecurityException("Invalid key file format");
                }
                throw new SecurityException("Invalid certificate format: " + certEx.getMessage());
            }
            
        } catch (SecurityException se) {
            throw se; 
        } catch (Exception e) {
            throw new SecurityException("Error validating certificate content: " + e.getMessage());
        }
    }
  
    private void validateTextCertificate(String content, String filename) throws SecurityException {
        
        if (content.contains("-----BEGIN CERTIFICATE-----") && 
            content.contains("-----END CERTIFICATE-----")) {
            
            try {
                String certContent = extractCertificateFromText(content);
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                cf.generateCertificate(new ByteArrayInputStream(certContent.getBytes()));
                logger.debug("Valid Couchbase certificate found in .txt file: {}", filename);
            } catch (Exception e) {
                logger.warn("Certificate validation failed for .txt file: {}", filename);
            }
            return;
        }
        if (isBase64Certificate(content)) {
            logger.debug("Base64 encoded certificate detected in .txt file: {}", filename);
            return;
        }
        
        if (content.trim().length() > 0 && !containsMaliciousPatterns(content)) {
            logger.info("Accepting .txt certificate file for Couchbase: {}", filename);
            return;
        }
        
        throw new SecurityException("Invalid certificate content in .txt file");
    }
    
    private String extractCertificateFromText(String content) {
        int beginIndex = content.indexOf("-----BEGIN CERTIFICATE-----");
        int endIndex = content.indexOf("-----END CERTIFICATE-----");
        
        if (beginIndex != -1 && endIndex != -1) {
            return content.substring(beginIndex, endIndex + "-----END CERTIFICATE-----".length());
        }
        return content;
    }
    
    private boolean isBase64Certificate(String content) {
        String trimmed = content.trim().replaceAll("\\s+", "");
        return trimmed.matches("^[A-Za-z0-9+/]+=*$") && trimmed.length() > 100;
    }
   
    private boolean containsMaliciousPatterns(String content) {
        String lowerContent = content.toLowerCase();
        return lowerContent.contains("<script") || 
               lowerContent.contains("javascript:") ||
               lowerContent.contains("eval(") ||
               lowerContent.contains("exec(") ||
               content.contains("\0"); // Null bytes
    }
    public void validateAndScanFile(MultipartFile file, String certificateType) throws SecurityException {
        logger.debug("Validating {} certificate", certificateType);
        validateAndScanFile(file);
        if ("couchbase".equalsIgnoreCase(certificateType)) {
            validateCouchbaseCertificate(file);
        } else if ("mongo".equalsIgnoreCase(certificateType)) {
            validateMongoCertificate(file);
        }
    }
    
    private void validateCouchbaseCertificate(MultipartFile file) throws SecurityException {
        String filename = file.getOriginalFilename();
        if (filename != null) {
            String lower = filename.toLowerCase();
            if (!lower.endsWith(".txt") && !lower.endsWith(".pem") && !lower.endsWith(".crt")) {
                logger.warn("Unusual file extension for Couchbase certificate: {}", filename);
            }
        }
    }
    
    private void validateMongoCertificate(MultipartFile file) throws SecurityException {
        String filename = file.getOriginalFilename();
        if (filename != null) {
            String lower = filename.toLowerCase();
            if (!lower.endsWith(".pem")) {
                logger.warn("MongoDB certificates are typically in .pem format, got: {}", filename);
            }
        }
    }
}