package com.demo.config;

import com.demo.security.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.header.writers.XXssProtectionHeaderWriter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, jsr250Enabled = true)
public class SecurityConfig {
	
    private static final Logger logger = LoggerFactory.getLogger(SecurityConfig.class);

    @Value("${spring.security.oauth2.resourceserver.jwt.jwk-set-uri}")
    private String jwkSetUri;
    
    @Value("${spring.security.oauth2.resourceserver.jwt.issuer-uri}")
    private String issuerUri;
    
    @Value("${security.cors.allowed-origins:http://localhost:3000,http://127.0.0.1:3000}")
    private String allowedOrigins;
    
    @Value("${security.enable-strict-mode:true}")
    private boolean enableStrictMode;
    
    @Autowired
    private Environment environment;
    
    @Autowired
    private AccountLockoutService lockoutService;
    
    @Autowired
    private SecurityEventService securityEventService;
    
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        logger.info("Configuring enhanced security for profile: {}", Arrays.toString(environment.getActiveProfiles()));
        
        http
            .csrf(csrf -> csrf
                .ignoringRequestMatchers(
                    new AntPathRequestMatcher("/ws-migration/**"),
                    new AntPathRequestMatcher("/ws-functions/**"),
                    new AntPathRequestMatcher("/ws/**"),  
                    new AntPathRequestMatcher("/sockjs-node/**") 
                )
                .csrfTokenRepository(org.springframework.security.web.csrf.CookieCsrfTokenRepository.withHttpOnlyFalse())
            )
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
            .headers(headers -> headers
                .contentSecurityPolicy(csp -> csp
                    .policyDirectives(getContentSecurityPolicy())
                )
                .frameOptions(frame -> frame.sameOrigin())
                .xssProtection(xss -> xss
                    .headerValue(XXssProtectionHeaderWriter.HeaderValue.ENABLED_MODE_BLOCK)
                )
                .httpStrictTransportSecurity(hsts -> {
                    if (isProduction()) {
                        hsts
                            .includeSubDomains(true)
                            .maxAgeInSeconds(31536000)
                            .preload(true);
                    }
                })
                .contentTypeOptions(contentType -> {})
                .referrerPolicy(referrer -> referrer
                    .policy(org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)
                )
                .permissionsPolicy(permissions -> permissions
                    .policy("geolocation=(self), microphone=(), camera=(), payment=(), usb=()")
                )
            )
            .sessionManagement(session -> session
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            )
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(
                    "/actuator/health",
                    "/actuator/info",
                    "/error",
                    "/ws-migration/**",     
                    "/ws-functions/**",      
                    "/ws/**",             
                    "/sockjs-node/**",
                    "/topic/**",          
                    "/queue/**",        
                    "/app/**"
                ).permitAll()
                .requestMatchers("/api/admin/**").authenticated() 
                .requestMatchers("/api/transfer/**").authenticated() 
                .requestMatchers("/api/**").authenticated()
                .anyRequest().authenticated()
            )
            .oauth2ResourceServer(oauth2 -> oauth2
                .jwt(jwt -> jwt.decoder(jwtDecoder()))
                .authenticationEntryPoint((request, response, authException) -> {
                    securityEventService.logSecurityEvent(
                        SecurityEventType.ACCESS_DENIED,
                        request.getRemoteAddr(),
                        "Unauthorized access attempt: " + request.getRequestURI(),
                        SecuritySeverity.HIGH
                    );
                    response.sendError(401, "Unauthorized");
                })
            )
          //  .addFilterBefore(new RateLimitingFilter(), UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(new SecurityHeadersFilter(), UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(new AuthenticationLockoutFilter(lockoutService), UsernamePasswordAuthenticationFilter.class);
        
        // Add HTTPS requirement for production
        if (isProduction()) {
            http.requiresChannel(channel -> 
                channel.anyRequest().requiresSecure()
            );
        }
        
        return http.build();
    }
    
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        
        List<String> origins = Arrays.asList(allowedOrigins.split(","));
        configuration.setAllowedOrigins(origins);
        
        logger.info("Configured CORS with allowed origins: {}", origins);
        
        configuration.setAllowedMethods(Arrays.asList(
            "GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "HEAD"
        ));
        
        configuration.setAllowedHeaders(Arrays.asList(
            "Authorization", "Content-Type", "X-Requested-With", "X-CSRF-Token",
            "Upgrade", "Connection", "Sec-WebSocket-Key",  
            "Sec-WebSocket-Version", "Sec-WebSocket-Extensions"
        ));
        
        configuration.setExposedHeaders(Arrays.asList(
            "Access-Control-Allow-Origin", 
            "Access-Control-Allow-Credentials",
            "Authorization",
            "Content-Disposition",
            "X-Total-Count",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
            "Upgrade", "Connection"
        ));
        
        configuration.setAllowCredentials(true);
        configuration.setMaxAge(3600L);
        
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    @Bean
    public JwtDecoder jwtDecoder() {
        logger.info("Configuring JWT decoder with JWK Set URI: {}", jwkSetUri);
        
        NimbusJwtDecoder jwtDecoder = NimbusJwtDecoder.withJwkSetUri(jwkSetUri)
            .build();
            
        jwtDecoder.setJwtValidator(JwtValidators.createDefaultWithIssuer(issuerUri));
        
        return jwtDecoder;
    }
    
    private String getContentSecurityPolicy() {
        if (isProduction()) {
            return "default-src 'self'; " +
                   "script-src 'self' 'strict-dynamic' 'nonce-{NONCE}'; " +
                   "style-src 'self' 'nonce-{NONCE}' https://fonts.googleapis.com; " +
                   "font-src 'self' https://fonts.gstatic.com; " +
                   "img-src 'self' data: https:; " +
                   "connect-src 'self' ws: wss: http://localhost:3000 https://localhost:3000 http://127.0.0.1:3000 https://127.0.0.1:3000; " + // ← SPECIFIC WEBSOCKET ORIGINS
                   "frame-ancestors 'none'; " +
                   "form-action 'self'; " +
                   "base-uri 'self'; " +
                   "object-src 'none';";
        } else {
            return "default-src 'self' 'unsafe-inline'; " +  // ← ADD 'unsafe-inline' for dev
                   "script-src 'self' 'unsafe-inline' 'unsafe-eval'; " +  // ← ADD for dev
                   "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; " +
                   "font-src 'self' https://fonts.gstatic.com; " +
                   "img-src 'self' data: blob:; " +
                   "connect-src 'self' ws: wss: http://localhost:* https://localhost:* http://127.0.0.1:* https://127.0.0.1:*; " + // ← WILDCARD FOR DEV
                   "frame-ancestors 'self'; " +
                   "form-action 'self'; " +
                   "base-uri 'self';";
        }
    }
    
    private boolean isProduction() {
        String[] profiles = environment.getActiveProfiles();
        if (profiles.length == 0) {
            profiles = environment.getDefaultProfiles();
        }
        
        for (String profile : profiles) {
            if (profile.toLowerCase().contains("prod") || 
                profile.toLowerCase().contains("production")) {
                return true;
            }
        }
        return false;
    }
}
