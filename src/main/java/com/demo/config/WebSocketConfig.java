//package com.demo.config
//
//import org.springframework.messaging.simp.config.MessageBrokerRegistry;
//import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
//import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
//import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@EnableWebSocketMessageBroker
//public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {
//    
////    @Override
////    public void configureMessageBroker(MessageBrokerRegistry config) {
////        config.enableSimpleBroker("/topic", "/queue");
////        config.setApplicationDestinationPrefixes("/app");
////        config.setUserDestinationPrefix("/user");
////    }
//	
//	@Override
//    public void configureMessageBroker(MessageBrokerRegistry registry) {
//        registry.setApplicationDestinationPrefixes("/app"); // 👈 For client -> server messages
//        registry.enableSimpleBroker("/topic");               // 👈 For server -> client messages
//    }
//
//    @Override
//    public void registerStompEndpoints(StompEndpointRegistry registry) {
//        registry.addEndpoint("/ws-migration")
//                .setAllowedOriginPatterns("http://localhost:3000")
//                .withSockJS();
//        
//        registry.addEndpoint("/ws-functions")
//                .setAllowedOriginPatterns("http://localhost:3000")
//                .withSockJS();
//    }
//}


package com.demo.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;

import com.demo.security.WebSocketSecurityConfig;

import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

	 @Autowired
	    private WebSocketSecurityConfig webSocketSecurityConfig;
	 @Override
	    public void configureClientInboundChannel(ChannelRegistration registration) {
	       
	        registration.interceptors(webSocketSecurityConfig.authenticationInterceptor());
	    }
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes("/app");
        registry.enableSimpleBroker("/topic", "/queue"); // Added /queue for direct messaging
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws-migration", "/ws-functions")
                .setAllowedOriginPatterns("http://localhost:3000", "http://127.0.0.1:3000")
                .withSockJS()
                .setSuppressCors(false); 

        registry.addEndpoint("/ws-migration", "/ws-functions")
                .setAllowedOriginPatterns("http://localhost:3000", "http://127.0.0.1:3000");
    }

    @Override
    public void configureWebSocketTransport(WebSocketTransportRegistration registration) {
        registration.setMessageSizeLimit(1024 * 1024); // 1MB max message size
        registration.setSendTimeLimit(20 * 10000); // 20 seconds
        registration.setSendBufferSizeLimit(1024 * 1024); // 1MB buffer
    }
}
