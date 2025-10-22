//package com.demo.service;
//
//
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.messaging.simp.SimpMessagingTemplate;
//import org.springframework.stereotype.Service;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class FunctionService {
//
//    @Autowired
//    private SimpMessagingTemplate messagingTemplate;
//
//    // Dummy method to simulate function migration
//    public void migrateFunctionsWithProgress() throws InterruptedException {
//        // Simulate source functions
//        List<String> sourceFunctions = List.of("func1", "func2", "func3", "func4", "func5");
//        int totalCount = sourceFunctions.size();
//
//        for (int i = 0; i < totalCount; i++) {
//            // Simulate function transfer logic
//            String function = sourceFunctions.get(i);
//            // TODO: Your actual function migration logic here
//
//            // Calculate progress
//            int migratedCount = i + 1;
//            int percentage = (int) ((migratedCount * 100.0) / totalCount);
//
//            // Send progress over WebSocket (with "type":"function" to differentiate)
//            Map<String, Object> progressPayload = new HashMap<>();
//            progressPayload.put("type", "function");  // Key difference from CollectionService
//            progressPayload.put("percentage", percentage);
//            progressPayload.put("migratedCount", migratedCount);
//            progressPayload.put("totalCount", totalCount);
//            progressPayload.put("status", migratedCount == totalCount ? "COMPLETED" : "IN_PROGRESS");
//            progressPayload.put("currentFunction", function);  // Additional function-specific info
//
//            messagingTemplate.convertAndSend("/topic/function-progress", progressPayload);
//
//            // Simulate processing delay
//            Thread.sleep(500); 
//        }
//    }
//}


package com.demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import com.demo.dto.FunctionTransferProgress;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class FunctionService {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    public void migrateFunctionsWithProgress(List<String> functions) throws InterruptedException {
        if (functions == null || functions.isEmpty()) {
            sendError("No functions to migrate");
            return;
        }

        final int totalCount = functions.size();
        final AtomicInteger successCount = new AtomicInteger(0);
        
        // Send initialization message
        sendProgress("STARTED", 0, totalCount, "Beginning function migration");

        for (int i = 0; i < totalCount; i++) {
            String functionName = functions.get(i);
            boolean success = false;
            
            try {
                // ========================================
                // ACTUAL MIGRATION LOGIC GOES HERE
                // ========================================
                success = migrateFunction(functionName); // Implement this method
                // ========================================
                
                if (success) {
                    successCount.incrementAndGet();
                }
                
                String status = (i == totalCount - 1) ? "COMPLETED" : "IN_PROGRESS";
                String message = success ? 
                    String.format("Migrated %s successfully", functionName) :
                    String.format("Failed to migrate %s", functionName);
                
                sendProgress(status, i + 1, totalCount, message);
                
            } catch (Exception e) {
                sendError(String.format("Error migrating %s: %s", 
                          functionName, e.getMessage()));
            }
            
            // Throttle processing
            Thread.sleep(500);
        }
        
        // Send final summary
        sendCompletionSummary(successCount.get(), totalCount);
    }

    private boolean migrateFunction(String functionName) {
        // TODO: Implement actual function migration logic
        // Return true if successful, false if failed
        return true; // Simulating success
    }

    private void sendProgress(String status, int processed, int total, String message) {
        FunctionTransferProgress progress = new FunctionTransferProgress(
            "Function Migration",
            status,
            processed,
            total,
            message
        );
        messagingTemplate.convertAndSend("/topic/function-progress", progress);
    }

    private void sendCompletionSummary(int successCount, int totalCount) {
        String message = String.format(
            "Completed with %d/%d functions migrated successfully",
            successCount, totalCount
        );
        FunctionTransferProgress completion = new FunctionTransferProgress(
            "Summary",
            "COMPLETED",
            totalCount,
            totalCount,
            message
        );
        messagingTemplate.convertAndSend("/topic/function-progress", completion);
    }

    private void sendError(String errorMessage) {
        FunctionTransferProgress error = new FunctionTransferProgress(
            "System",
            "ERROR",
            0,
            0,
            errorMessage
        );
        messagingTemplate.convertAndSend("/topic/function-errors", error);
    }
}
