package com.demo.security;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class WebSocketSecurityConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(WebSocketSecurityConfig.class);
    
    @Autowired
    private JwtDecoder jwtDecoder;
    
    @Bean
    public ChannelInterceptor authenticationInterceptor() {
        return new ChannelInterceptor() {
            @Override
            public Message<?> preSend(Message<?> message, MessageChannel channel) {
                StompHeaderAccessor accessor = 
                    MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
                
                if (StompCommand.CONNECT.equals(accessor.getCommand())) {
                    String token = accessor.getFirstNativeHeader("Authorization");
                    
                    if (token != null && token.startsWith("Bearer ")) {
                        try {
                            // Validate JWT token
                            Authentication auth = validateToken(token.substring(7));
                            accessor.setUser(auth);
                            logger.debug("WebSocket authentication successful for user: {}", auth.getName());
                        } catch (Exception e) {
                            logger.error("WebSocket authentication failed: {}", e.getMessage());
                            throw new MessageDeliveryException("Invalid authentication token");
                        }
                    } else {
                        logger.warn("WebSocket connection attempt without authentication token");
                        throw new MessageDeliveryException("Missing authentication token");
                    }
                }
                return message;
            }
            
            private Authentication validateToken(String token) {
                try {
                    if (token == null || token.length() < 100) {
                        throw new SecurityException("Invalid token length");
                    }
                    
                    // Decode and validate the JWT token
                    Jwt jwt = jwtDecoder.decode(token);
                    
                    // Additional token validation
                    validateTokenClaims(jwt);
                    
                    // Extract user information from JWT
                    String username = jwt.getSubject();
                    if (username == null) {
                        username = jwt.getClaim("sub");
                    }
                    
                    // Extract roles/authorities from JWT
                    List<SimpleGrantedAuthority> authorities = extractAuthorities(jwt);
                    
                    // Create authentication object
                    return new UsernamePasswordAuthenticationToken(
                        username, 
                        null, 
                        authorities
                    );
                } catch (JwtException e) {
                    logger.error("JWT validation failed: {}", e.getMessage());
                    throw new SecurityException("Invalid JWT token", e);
                }
            }
            
            private void validateTokenClaims(Jwt jwt) {
                // Validate expiration
                Instant expiration = jwt.getExpiresAt();
                if (expiration != null && expiration.isBefore(Instant.now())) {
                    throw new SecurityException("Token expired");
                }
                
                // Validate issuer
                String issuer = jwt.getIssuer() != null ? jwt.getIssuer().toString() : "";
                if (!issuer.equals("https://dev-qyezg6s35kzzqg7v.us.auth0.com/")) {
                    throw new SecurityException("Invalid token issuer");
                }
            }
            
            private List<SimpleGrantedAuthority> extractAuthorities(Jwt jwt) {
                // Try different claim names for roles/authorities
                List<String> roles = null;
                
                // Try "roles" claim
                if (jwt.getClaims().containsKey("roles")) {
                    roles = jwt.getClaim("roles");
                }
                // Try "authorities" claim
                else if (jwt.getClaims().containsKey("authorities")) {
                    roles = jwt.getClaim("authorities");
                }
                // Try "scope" claim (OAuth2 style)
                else if (jwt.getClaims().containsKey("scope")) {
                    String scope = jwt.getClaim("scope");
                    if (scope != null) {
                        roles = List.of(scope.split(" "));
                    }
                }
                
                if (roles != null && !roles.isEmpty()) {
                    return roles.stream()
                        .map(role -> new SimpleGrantedAuthority("ROLE_" + role.toUpperCase()))
                        .collect(Collectors.toList());
                }
                
                // Default role if no roles found
                return Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER"));
            }
        };
    }
    
    // Custom Principal implementation for WebSocket
    public static class WebSocketPrincipal implements Principal {
        private final String name;
        private final Authentication authentication;
        
        public WebSocketPrincipal(String name, Authentication authentication) {
            this.name = name;
            this.authentication = authentication;
        }
        
        @Override
        public String getName() {
            return name;
        }
        
        public Authentication getAuthentication() {
            return authentication;
        }
    }
}