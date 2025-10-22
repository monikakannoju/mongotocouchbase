//package com.demo.config;
//
//import jakarta.servlet.*;
//import jakarta.servlet.http.HttpServletRequest;
//import jakarta.servlet.http.HttpServletResponse;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.http.HttpStatus;
//
//import java.io.IOException;
//import java.time.Duration;
//import java.time.Instant;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ScheduledExecutorService;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class RateLimitingFilter implements Filter {
//    
//    private static final Logger logger = LoggerFactory.getLogger(RateLimitingFilter.class);
//    
//    private static final int DEFAULT_REQUESTS_PER_MINUTE = 60;
//    private static final int DEFAULT_REQUESTS_PER_HOUR = 1000;
//    private static final Duration CLEANUP_INTERVAL = Duration.ofMinutes(5);
//    
//    private final Map<String, RateLimitBucket> minuteBuckets = new ConcurrentHashMap<>();
//    private final Map<String, RateLimitBucket> hourBuckets = new ConcurrentHashMap<>();
//    
//    private ScheduledExecutorService cleanupExecutor;
//    
//    @Override
//    public void init(FilterConfig filterConfig) throws ServletException {
//        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
//            Thread thread = new Thread(r, "RateLimitCleanup");
//            thread.setDaemon(true);
//            return thread;
//        });
//        cleanupExecutor.scheduleAtFixedRate(
//            this::cleanupExpiredBuckets,
//            CLEANUP_INTERVAL.toMillis(),
//            CLEANUP_INTERVAL.toMillis(),
//            TimeUnit.MILLISECONDS
//        );
//        
//        logger.info("RateLimitingFilter initialized with limits: {}/min, {}/hour", 
//            DEFAULT_REQUESTS_PER_MINUTE, DEFAULT_REQUESTS_PER_HOUR);
//    }
//    
//    @Override
//    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
//            throws IOException, ServletException {
//        
//        HttpServletRequest httpRequest = (HttpServletRequest) request;
//        HttpServletResponse httpResponse = (HttpServletResponse) response;
//        
//        String clientIdentifier = getClientIdentifier(httpRequest);
//        if (!checkRateLimit(clientIdentifier, minuteBuckets, DEFAULT_REQUESTS_PER_MINUTE, Duration.ofMinutes(1))) {
//            handleRateLimitExceeded(httpResponse, "Rate limit exceeded: too many requests per minute");
//            return;
//        }
//        
//        if (!checkRateLimit(clientIdentifier, hourBuckets, DEFAULT_REQUESTS_PER_HOUR, Duration.ofHours(1))) {
//            handleRateLimitExceeded(httpResponse, "Rate limit exceeded: too many requests per hour");
//            return;
//        }
//        
//        addRateLimitHeaders(httpResponse, clientIdentifier);
//        chain.doFilter(request, response);
//    }
//    
//    @Override
//    public void destroy() {
//        if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
//            cleanupExecutor.shutdown();
//            try {
//                if (!cleanupExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
//                    cleanupExecutor.shutdownNow();
//                }
//            } catch (InterruptedException e) {
//                cleanupExecutor.shutdownNow();
//                Thread.currentThread().interrupt();
//            }
//        }
//        
//        minuteBuckets.clear();
//        hourBuckets.clear();
//        logger.info("RateLimitingFilter destroyed");
//    }
//    private String getClientIdentifier(HttpServletRequest request) {
//
//        if (request.getUserPrincipal() != null) {
//            return "user:" + request.getUserPrincipal().getName();
//        }
//        
//        String clientIp = request.getHeader("X-Forwarded-For");
//        if (clientIp == null || clientIp.isEmpty()) {
//            clientIp = request.getHeader("X-Real-IP");
//        }
//        if (clientIp == null || clientIp.isEmpty()) {
//            clientIp = request.getRemoteAddr();
//        }
//        if (clientIp != null && clientIp.contains(",")) {
//            clientIp = clientIp.split(",")[0].trim();
//        }
//        
//        return "ip:" + clientIp;
//    }
//    
//    private boolean checkRateLimit(String identifier, Map<String, RateLimitBucket> buckets, 
//                                  int limit, Duration window) {
//        Instant now = Instant.now();
//        
//        RateLimitBucket bucket = buckets.compute(identifier, (key, existing) -> {
//            if (existing == null || existing.isExpired(now, window)) {
//                return new RateLimitBucket(now);
//            }
//            return existing;
//        });
//        
//        return bucket.tryConsume(limit);
//    }
//    private void handleRateLimitExceeded(HttpServletResponse response, String message) 
//            throws IOException {
//        logger.warn("Rate limit exceeded: {}", message);
//        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
//        response.setContentType("application/json");
//        response.getWriter().write(String.format(
//            "{\"error\": \"%s\", \"status\": %d}", 
//            message, 
//            HttpStatus.TOO_MANY_REQUESTS.value()
//        ));
//    }
//    private void addRateLimitHeaders(HttpServletResponse response, String identifier) {
//        RateLimitBucket minuteBucket = minuteBuckets.get(identifier);
//        if (minuteBucket != null) {
//            response.addHeader("X-RateLimit-Limit", String.valueOf(DEFAULT_REQUESTS_PER_MINUTE));
//            response.addHeader("X-RateLimit-Remaining", 
//                String.valueOf(Math.max(0, DEFAULT_REQUESTS_PER_MINUTE - minuteBucket.getCount())));
//            response.addHeader("X-RateLimit-Reset", 
//                String.valueOf(minuteBucket.getResetTime(Duration.ofMinutes(1))));
//        }
//    }
//    private void cleanupExpiredBuckets() {
//        Instant now = Instant.now();
//        
//        minuteBuckets.entrySet().removeIf(entry -> 
//            entry.getValue().isExpired(now, Duration.ofMinutes(2)));
//        
//        hourBuckets.entrySet().removeIf(entry -> 
//            entry.getValue().isExpired(now, Duration.ofHours(2)));
//        
//        logger.debug("Cleaned up expired rate limit buckets. Active: {} minute, {} hour", 
//            minuteBuckets.size(), hourBuckets.size());
//    }
//    private static class RateLimitBucket {
//        private final Instant createdAt;
//        private final AtomicInteger count;
//        
//        public RateLimitBucket(Instant createdAt) {
//            this.createdAt = createdAt;
//            this.count = new AtomicInteger(0);
//        }
//        
//        public boolean tryConsume(int limit) {
//            int currentCount = count.incrementAndGet();
//            return currentCount <= limit;
//        }
//        
//        public int getCount() {
//            return count.get();
//        }
//        
//        public boolean isExpired(Instant now, Duration window) {
//            return Duration.between(createdAt, now).compareTo(window) > 0;
//        }
//        
//        public long getResetTime(Duration window) {
//            return createdAt.plus(window).toEpochMilli();
//        }
//    }
//}