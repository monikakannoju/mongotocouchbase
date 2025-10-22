package com.demo.security;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;

public class SecurityHeadersFilter implements Filter {
    
    private static final Logger logger = LoggerFactory.getLogger(SecurityHeadersFilter.class);
    
    private static final String NONCE_REQUEST_ATTRIBUTE = "CSP_NONCE";
    private static final int NONCE_SIZE = 16;
    
    private final SecureRandom secureRandom = new SecureRandom();
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("SecurityHeadersFilter initialized");
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        
        // Generate CSP nonce for this request
        String nonce = generateNonce();
        httpRequest.setAttribute(NONCE_REQUEST_ATTRIBUTE, nonce);
        
        // Add security headers before processing the request
        addSecurityHeaders(httpResponse, nonce);
        
        // Add additional security headers based on content type
        if (isApiRequest(httpRequest)) {
            addApiSecurityHeaders(httpResponse);
        }
        
        // Log security header application for debugging
        if (logger.isDebugEnabled()) {
            logger.debug("Applied security headers to request: {} {}", 
                httpRequest.getMethod(), httpRequest.getRequestURI());
        }
        
        // Continue with the request
        chain.doFilter(request, response);
    }
    
    @Override
    public void destroy() {
        logger.info("SecurityHeadersFilter destroyed");
    }
    
    /**
     * Add standard security headers to response
     */
    private void addSecurityHeaders(HttpServletResponse response, String nonce) {
        // Prevent browsers from MIME-sniffing
        response.setHeader("X-Content-Type-Options", "nosniff");
        
        // Enable browser XSS protection
        response.setHeader("X-XSS-Protection", "1; mode=block");
        
        // Prevent clickjacking
        response.setHeader("X-Frame-Options", "DENY");
        
        // Referrer Policy
        response.setHeader("Referrer-Policy", "strict-origin-when-cross-origin");
        
        // Feature Policy / Permissions Policy
        response.setHeader("Permissions-Policy", 
            "geolocation=(self), microphone=(), camera=(), payment=(), usb=(), magnetometer=(), " +
            "gyroscope=(), accelerometer=(), ambient-light-sensor=(), autoplay=(), " +
            "encrypted-media=(), picture-in-picture=()");
        
        // Content Security Policy with nonce
        String cspHeader = buildContentSecurityPolicy(nonce);
        response.setHeader("Content-Security-Policy", cspHeader);
        
        // Strict Transport Security (HSTS) - only for HTTPS
        // This is typically handled in SecurityConfig for production environments
        
        // Cache Control for sensitive pages
        if (!isStaticResource(response)) {
            response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
            response.setHeader("Pragma", "no-cache");
            response.setHeader("Expires", "0");
        }
        
        // Additional security headers
        response.setHeader("X-Permitted-Cross-Domain-Policies", "none");
        response.setHeader("X-Download-Options", "noopen");
        response.setHeader("X-DNS-Prefetch-Control", "off");
    }
    
    /**
     * Add API-specific security headers
     */
    private void addApiSecurityHeaders(HttpServletResponse response) {
        // API responses should not be cached by default
        response.setHeader("Cache-Control", "no-store, must-revalidate");
        
        // API version header (optional)
        response.setHeader("X-API-Version", "1.0");
        
        // Request ID for tracking (could be generated per request)
        response.setHeader("X-Request-ID", generateRequestId());
    }
    
    /**
     * Build Content Security Policy header value
     */
    private String buildContentSecurityPolicy(String nonce) {
        StringBuilder csp = new StringBuilder();
        
        // Default source
        csp.append("default-src 'self'; ");
        
        // Script source with nonce
        csp.append("script-src 'self' 'nonce-").append(nonce).append("' ");
        csp.append("https://cdn.jsdelivr.net https://unpkg.com; ");
        
        // Style source
        csp.append("style-src 'self' 'nonce-").append(nonce).append("' ");
        csp.append("https://fonts.googleapis.com https://cdn.jsdelivr.net; ");
        
        // Font source
        csp.append("font-src 'self' https://fonts.gstatic.com data:; ");
        
        // Image source
        csp.append("img-src 'self' data: https: blob:; ");
        
        // Connect source (for AJAX, WebSocket, etc.)
        csp.append("connect-src 'self' wss: https:; ");
        
        // Frame ancestors
        csp.append("frame-ancestors 'none'; ");
        
        // Form action
        csp.append("form-action 'self'; ");
        
        // Base URI
        csp.append("base-uri 'self'; ");
        
        // Object source
        csp.append("object-src 'none'; ");
        
        // Media source
        csp.append("media-src 'self'; ");
        
        // Worker source
        csp.append("worker-src 'self' blob:; ");
        
        // Manifest source
        csp.append("manifest-src 'self'; ");
        
        // Upgrade insecure requests (for HTTPS migration)
        csp.append("upgrade-insecure-requests; ");
        
        // Block all mixed content
        csp.append("block-all-mixed-content;");
        
        return csp.toString();
    }
    
    /**
     * Generate a cryptographically secure nonce
     */
    private String generateNonce() {
        byte[] nonceBytes = new byte[NONCE_SIZE];
        secureRandom.nextBytes(nonceBytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(nonceBytes);
    }
    
    /**
     * Generate a request ID for tracking
     */
    private String generateRequestId() {
        return java.util.UUID.randomUUID().toString();
    }
    
    /**
     * Check if the request is for an API endpoint
     */
    private boolean isApiRequest(HttpServletRequest request) {
        String path = request.getRequestURI();
        return path != null && (path.startsWith("/api/") || path.startsWith("/ws-"));
    }
    
    /**
     * Check if the response is for a static resource
     */
    private boolean isStaticResource(HttpServletResponse response) {
        String contentType = response.getContentType();
        if (contentType == null) {
            return false;
        }
        
        return contentType.startsWith("image/") ||
               contentType.startsWith("text/css") ||
               contentType.startsWith("application/javascript") ||
               contentType.startsWith("font/") ||
               contentType.equals("application/x-font-woff") ||
               contentType.equals("application/font-woff2");
    }
}