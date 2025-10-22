package com.demo.security;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class EncryptionService {
    private static final Logger logger = LoggerFactory.getLogger(EncryptionService.class);
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;
    
    @Value("${security.encryption.key:}")
    private String encodedKey;
    
    private SecretKey key;
    private String currentKeyBase64;
    
    @PostConstruct
    public void init() {
        this.key = loadOrGenerateKey();
        this.currentKeyBase64 = encodedKey;
    }
    
    /**
     * Load a SecretKey from a Base64-encoded string
     * @param base64Key The Base64-encoded key string
     * @return SecretKey object
     */
    private SecretKey loadKeyFromBase64(String base64Key) {
        try {
            if (base64Key == null || base64Key.trim().isEmpty()) {
                throw new IllegalArgumentException("Base64 key cannot be null or empty");
            }
            
            byte[] decodedKey = Base64.getDecoder().decode(base64Key.trim());
            
            // Validate key length (AES requires 128, 192, or 256 bits)
            if (decodedKey.length != 16 && decodedKey.length != 24 && decodedKey.length != 32) {
                throw new IllegalArgumentException("Invalid AES key length: " + decodedKey.length + " bytes. " +
                    "Must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256");
            }
            
            SecretKey secretKey = new SecretKeySpec(decodedKey, "AES");
            logger.info("Successfully loaded AES-{} key from Base64", decodedKey.length * 8);
            
            return secretKey;
        } catch (IllegalArgumentException e) {
            logger.error("Failed to decode Base64 key: {}", e.getMessage());
            throw new SecurityException("Invalid Base64 key format", e);
        } catch (Exception e) {
            logger.error("Failed to load key from Base64: {}", e.getMessage());
            throw new SecurityException("Failed to load encryption key", e);
        }
    }
    
    /**
     * Update the encryption key with a new Base64-encoded key
     * @param newKeyBase64 The new Base64-encoded key
     */
    public void updateKey(String newKeyBase64) {
        if (newKeyBase64 == null || newKeyBase64.trim().isEmpty()) {
            throw new IllegalArgumentException("New key cannot be null or empty");
        }
        
        try {
            SecretKey newKey = loadKeyFromBase64(newKeyBase64);
            
            // Validate the new key by trying to encrypt/decrypt a test string
            validateKey(newKey);
            
            // Update the key if validation passes
            this.currentKeyBase64 = newKeyBase64;
            this.key = newKey;
            
            logger.info("Encryption key successfully updated");
        } catch (Exception e) {
            logger.error("Failed to update encryption key: {}", e.getMessage());
            throw new SecurityException("Failed to update encryption key", e);
        }
    }
    
    /**
     * Get the current Base64-encoded key
     * @return The current key as Base64 string
     */
    public String getCurrentKeyBase64() {
        return currentKeyBase64;
    }
    
    /**
     * Generate a new AES key and return it as Base64
     * @param keySize The key size in bits (128, 192, or 256)
     * @return Base64-encoded key
     */
    public static String generateNewKeyBase64(int keySize) {
        if (keySize != 128 && keySize != 192 && keySize != 256) {
            throw new IllegalArgumentException("Key size must be 128, 192, or 256 bits");
        }
        
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(keySize);
            SecretKey generatedKey = keyGenerator.generateKey();
            String base64Key = Base64.getEncoder().encodeToString(generatedKey.getEncoded());
            
            logger.info("Generated new AES-{} key", keySize);
            return base64Key;
        } catch (Exception e) {
            throw new SecurityException("Failed to generate new key", e);
        }
    }
    
    /**
     * Validate a key by performing test encryption/decryption
     */
    private void validateKey(SecretKey keyToValidate) {
        try {
            String testData = "ValidationTest";
            
            // Test encryption
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, keyToValidate, parameterSpec);
            byte[] encrypted = cipher.doFinal(testData.getBytes("UTF-8"));
            
            // Test decryption
            cipher.init(Cipher.DECRYPT_MODE, keyToValidate, parameterSpec);
            byte[] decrypted = cipher.doFinal(encrypted);
            String decryptedString = new String(decrypted, "UTF-8");
            
            if (!testData.equals(decryptedString)) {
                throw new SecurityException("Key validation failed: encryption/decryption test failed");
            }
        } catch (Exception e) {
            throw new SecurityException("Key validation failed", e);
        }
    }
    
    private SecretKey loadOrGenerateKey() {
        try {
            if (encodedKey != null && !encodedKey.isEmpty()) {
                return loadKeyFromBase64(encodedKey);
            } else {
                logger.warn("No encryption key configured, generating new key. THIS IS NOT SECURE FOR PRODUCTION!");
                KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
                keyGenerator.init(256);
                SecretKey generatedKey = keyGenerator.generateKey();
                String generatedKeyBase64 = Base64.getEncoder().encodeToString(generatedKey.getEncoded());
                logger.info("Generated encryption key (save this in configuration): {}", generatedKeyBase64);
                
                this.currentKeyBase64 = generatedKeyBase64;
                return generatedKey;
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize encryption key", e);
        }
    }
    
    public String encryptSensitive(String plainText) {
        if (plainText == null || plainText.isEmpty()) {
            return plainText;
        }
        
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, parameterSpec);
            byte[] encrypted = cipher.doFinal(plainText.getBytes("UTF-8"));
            
            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + encrypted.length);
            byteBuffer.put(iv);
            byteBuffer.put(encrypted);
            
            return Base64.getEncoder().encodeToString(byteBuffer.array());
        } catch (Exception e) {
            throw new SecurityException("Encryption failed", e);
        }
    }
    
    public String decryptSensitive(String encryptedText) {
        if (encryptedText == null || encryptedText.isEmpty()) {
            return encryptedText;
        }
        
        try {
            byte[] cipherMessage = Base64.getDecoder().decode(encryptedText);
            ByteBuffer byteBuffer = ByteBuffer.wrap(cipherMessage);
            
            byte[] iv = new byte[GCM_IV_LENGTH];
            byteBuffer.get(iv);
            byte[] encrypted = new byte[byteBuffer.remaining()];
            byteBuffer.get(encrypted);
            
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
            
            byte[] decrypted = cipher.doFinal(encrypted);
            return new String(decrypted, "UTF-8");
        } catch (Exception e) {
            throw new SecurityException("Decryption failed", e);
        }
    }
    
    public byte[] encryptBytes(byte[] plainBytes) {
        if (plainBytes == null || plainBytes.length == 0) {
            return plainBytes;
        }
        
        try {
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);
            
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.ENCRYPT_MODE, key, parameterSpec);
            byte[] encrypted = cipher.doFinal(plainBytes);
            
            ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + encrypted.length);
            byteBuffer.put(iv);
            byteBuffer.put(encrypted);
            
            return byteBuffer.array();
        } catch (Exception e) {
            throw new SecurityException("Byte encryption failed", e);
        }
    }
    
    public byte[] decryptBytes(byte[] encryptedBytes) {
        if (encryptedBytes == null || encryptedBytes.length == 0) {
            return encryptedBytes;
        }
        
        try {
            ByteBuffer byteBuffer = ByteBuffer.wrap(encryptedBytes);
            
            byte[] iv = new byte[GCM_IV_LENGTH];
            byteBuffer.get(iv);
            byte[] encrypted = new byte[byteBuffer.remaining()];
            byteBuffer.get(encrypted);
            
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, key, parameterSpec);
            
            return cipher.doFinal(encrypted);
        } catch (Exception e) {
            throw new SecurityException("Byte decryption failed", e);
        }
    }
    
    public boolean isEncrypted(String text) {
        if (text == null || text.isEmpty()) {
            return false;
        }
        
        try {
            byte[] decoded = Base64.getDecoder().decode(text);
            // Check if the decoded bytes have at least the IV length plus some encrypted content
            return decoded.length > GCM_IV_LENGTH;
        } catch (Exception e) {
            return false;
        }
    }
}