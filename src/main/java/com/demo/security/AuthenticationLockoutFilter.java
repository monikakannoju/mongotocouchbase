package com.demo.security;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;

import java.io.IOException;

public class AuthenticationLockoutFilter implements Filter {
    
    private final AccountLockoutService lockoutService;
    
    public AuthenticationLockoutFilter(AccountLockoutService lockoutService) {
        this.lockoutService = lockoutService;
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
            throws IOException, ServletException {
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof Jwt) {
            Jwt jwt = (Jwt) auth.getPrincipal();
            String userId = jwt.getClaim("sub");
            
            if (lockoutService.isAccountLocked(userId)) {
                httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
                httpResponse.getWriter().write("Account is locked due to too many failed attempts");
                return;
            }
        }
        
        chain.doFilter(request, response);
    }
}