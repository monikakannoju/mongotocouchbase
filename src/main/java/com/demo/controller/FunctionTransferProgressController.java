package com.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import com.demo.dto.FunctionTransferProgress;

@Controller
public class FunctionTransferProgressController {
    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    private volatile int totalFunctions = 0;
    private volatile int processedFunctions = 0;
    private volatile boolean isCompleted = false;

    public synchronized void startMigration(int totalCount) {
        if (totalCount <= 0) {
            throw new IllegalArgumentException("Total functions must be positive");
        }
        this.totalFunctions = totalCount;
        this.processedFunctions = 0;
        this.isCompleted = false;
        sendProgress("STARTED", "Migration initialized with " + totalCount + " functions");
    }

    public synchronized void updateFunctionProgress(String functionName, boolean success) {
        if (isCompleted) {
            return; // Ignore updates after completion
        }

        if (processedFunctions >= totalFunctions) {
            sendFunctionError("System", "Received progress update beyond total count");
            return;
        }

        processedFunctions++;
        String status = processedFunctions >= totalFunctions ? "COMPLETED" : "IN_PROGRESS";
        String message = success ? "Processed " + functionName : "Failed to process " + functionName;
        sendProgress(status, message);
        if (status.equals("COMPLETED")) {
            isCompleted = true;
            sendCompletionSummary();
        }
    }

    private void sendProgress(String status, String message) {
        FunctionTransferProgress progress = new FunctionTransferProgress(
            "Overall Progress",
            status,
            processedFunctions,
            totalFunctions,
            message
        );
        System.out.printf("[Progress] %d/%d (%s) - %s%n",
            processedFunctions, totalFunctions, status, message);
        messagingTemplate.convertAndSend("/topic/function-progress", progress);
    }

    private void sendCompletionSummary() {
        FunctionTransferProgress completion = new FunctionTransferProgress(
            "Summary",
            "COMPLETED",
            processedFunctions,
            totalFunctions,
            "Processed " + processedFunctions + "/" + totalFunctions + " functions"
        );
        messagingTemplate.convertAndSend("/topic/function-progress", completion);
    }

    public void sendFunctionError(String functionName, Exception error) {
        FunctionTransferProgress errorProgress = new FunctionTransferProgress(
            functionName,
            "ERROR",
            processedFunctions,
            totalFunctions,
            error.getMessage() != null ? error.getMessage() : "Unknown error"
        );
        messagingTemplate.convertAndSend("/topic/function-errors", errorProgress);
        System.err.println("[Error] " + errorProgress.getMessage());
    }

    public void sendFunctionError(String functionName, String errorMessage) {
        FunctionTransferProgress errorProgress = new FunctionTransferProgress(
            functionName,
            "ERROR",
            processedFunctions,
            totalFunctions,
            errorMessage != null ? errorMessage : "Unknown error"
        );
        messagingTemplate.convertAndSend("/topic/function-errors", errorProgress);
        System.err.println("[Error] " + errorProgress.getMessage());
    }

    public void sendFunctionProgress(String functionName, String status, 
                                   int processed, int total, String message) {
        FunctionTransferProgress progress = new FunctionTransferProgress(
            functionName, status, processed, total, message
        );
        messagingTemplate.convertAndSend("/topic/function-progress", progress);
    }
}