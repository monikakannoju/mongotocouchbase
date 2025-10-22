package com.demo.security;

import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Service
public class AccountLockoutService {
    
    private static final Logger logger = LoggerFactory.getLogger(AccountLockoutService.class);
    
    private static final int MAX_FAILED_ATTEMPTS = 5;
    private static final Duration LOCKOUT_DURATION = Duration.ofMinutes(30);
    
    private final Map<String, LockoutInfo> lockoutMap = new ConcurrentHashMap<>();
    public boolean isAccountLocked(String userId) {
        if (userId == null) {
            return false;
        }
        
        LockoutInfo lockoutInfo = lockoutMap.get(userId);
        
        if (lockoutInfo == null) {
            return false;
        }
        
        if (lockoutInfo.isLocked()) {
            if (lockoutInfo.isLockoutExpired()) {
                // Lockout period has expired, reset the account
                resetAccount(userId);
                logger.info("Account lockout expired for user: {}", userId);
                return false;
            }
            return true;
        }
        
        return false;
    }
    
    public void recordFailedAttempt(String userId) {
        if (userId == null) {
            return;
        }
        
        lockoutMap.compute(userId, (key, existingInfo) -> {
            LockoutInfo info = existingInfo != null ? existingInfo : new LockoutInfo();
            info.incrementFailedAttempts();
            
            if (info.getFailedAttempts() >= MAX_FAILED_ATTEMPTS) {
                info.lockAccount();
                logger.warn("Account locked for user: {} after {} failed attempts", 
                    userId, MAX_FAILED_ATTEMPTS);
            }
            
            return info;
        });
    }
    public void recordSuccessfulLogin(String userId) {
        if (userId == null) {
            return;
        }
        
        resetAccount(userId);
        logger.debug("Successful login recorded for user: {}", userId);
    }
    public void resetAccount(String userId) {
        if (userId != null) {
            lockoutMap.remove(userId);
        }
    }
    public long getRemainingLockoutTime(String userId) {
        if (userId == null) {
            return 0;
        }
        
        LockoutInfo lockoutInfo = lockoutMap.get(userId);
        
        if (lockoutInfo != null && lockoutInfo.isLocked()) {
            Duration remaining = Duration.between(LocalDateTime.now(), lockoutInfo.getLockoutEndTime());
            return Math.max(0, remaining.toMinutes());
        }
        
        return 0;
    }
    public int getFailedAttempts(String userId) {
        if (userId == null) {
            return 0;
        }
        
        LockoutInfo lockoutInfo = lockoutMap.get(userId);
        return lockoutInfo != null ? lockoutInfo.getFailedAttempts() : 0;
    }
    private static class LockoutInfo {
        private int failedAttempts = 0;
        private LocalDateTime lockoutStartTime;
        private LocalDateTime lockoutEndTime;
        private boolean locked = false;
        
        public void incrementFailedAttempts() {
            failedAttempts++;
        }
        
        public void lockAccount() {
            locked = true;
            lockoutStartTime = LocalDateTime.now();
            lockoutEndTime = lockoutStartTime.plus(LOCKOUT_DURATION);
        }
        
        public boolean isLocked() {
            return locked;
        }
        
        public boolean isLockoutExpired() {
            return lockoutEndTime != null && LocalDateTime.now().isAfter(lockoutEndTime);
        }
        
        public int getFailedAttempts() {
            return failedAttempts;
        }
        
        public LocalDateTime getLockoutEndTime() {
            return lockoutEndTime;
        }
    }
}