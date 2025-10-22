package com.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.demo.dto.CdcEvent;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/test")
public class CdcWebsocketController {
    
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    @PostMapping("/send-test-cdc")
    public String sendTestCdcEvent() {
        try {
            // Create a test CDC event
            Map<String, Object> documentData = new HashMap<>();
            documentData.put("name", "Test Document");
            documentData.put("value", 123);
            documentData.put("test", true);
            
            CdcEvent testEvent = new CdcEvent("INSERT", "test_db", "test_collection");
            testEvent.setDocumentId("TEST_DOC_123");
            testEvent.setAfter(documentData);
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("test", true);
            metadata.put("timestamp", System.currentTimeMillis());
            testEvent.setMetadata(metadata);
            
            messagingTemplate.convertAndSend("/topic/cdc-events", testEvent);
            
            return "✅ Test CDC event sent successfully! Check your frontend.";
        } catch (Exception e) {
            return "❌ Error sending test event: " + e.getMessage();
        }
    }
    
    @PostMapping("/send-test-delete")
    public String sendTestDeleteEvent() {
        try {
            // Create a test DELETE event
            Map<String, Object> documentData = new HashMap<>();
            documentData.put("name", "Deleted Document");
            documentData.put("value", 456);
            
            CdcEvent testEvent = new CdcEvent("DELETE", "test_db", "test_collection");
            testEvent.setDocumentId("TEST_DOC_DEL_456");
            testEvent.setBefore(documentData);
            
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("test", true);
            metadata.put("timestamp", System.currentTimeMillis());
            testEvent.setMetadata(metadata);
            
            messagingTemplate.convertAndSend("/topic/cdc-events", testEvent);
            
            return "✅ Test DELETE event sent successfully!";
        } catch (Exception e) {
            return "❌ Error sending test event: " + e.getMessage();
        }
    }
}
