package com.demo.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import com.demo.dto.DeltaMigration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

@Service
public class DeltaService {
    private List<DeltaMigration> migrationSteps;
    private ScheduledExecutorService scheduler;
    private boolean simulationRunning = false;
    
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:mm:ss a");
    
    @PostConstruct
    public void init() {
        initializeMigrationSteps();
        scheduler = Executors.newScheduledThreadPool(1);
    }
    
    public List<DeltaMigration> initializeMigrationSteps() {
        migrationSteps = new ArrayList<>();
        migrationSteps.add(new DeltaMigration("1", "Not started", "Full Data Migration", 
                "MongoDB to Couchbase", "pending", 0, "Application connected to MongoDB"));
        migrationSteps.add(new DeltaMigration("2", "Not started", "All Prechecks for Application failover", 
                "Application still Connected to Mongo", "pending", 0, "Application connected to MongoDB"));
        migrationSteps.add(new DeltaMigration("3", "Not started", "Run CDC to capture data", 
                "Application still Connected to Mongo", "pending", 0, "Application connected to MongoDB"));
        migrationSteps.add(new DeltaMigration("4", "Not started", "Application switchover to Couchbase Cluster", 
                "Application is Connected to Couchbase", "pending", 0, "Application connected to MongoDB"));
        return migrationSteps;
    }
    
    public List<DeltaMigration> getMigrationStatus() {
        return migrationSteps;
    }
    
    public void startMigrationSimulation() {
        if (simulationRunning) {
            return;
        }
        
        simulationRunning = true;
        
        // Get current time for the first step
        String currentTime = LocalTime.now().format(timeFormatter);
        updateStepStatus("1", "in-progress", 0, "Starting data migration...", "Application connected to MongoDB", currentTime);
        
        // Simulate first step: Full Data Migration
        scheduler.schedule(() -> {
            String completionTime = LocalTime.now().format(timeFormatter);
            updateStepStatus("1", "completed", 100, "Data migration complete", "Application connected to MongoDB", completionTime);
            
            // Start second step immediately after first completes
            String startTimeStep2 = LocalTime.now().format(timeFormatter);
            updateStepStatus("2", "in-progress", 0, "Starting prechecks...", "Application connected to MongoDB", startTimeStep2);
            
        }, 10000, TimeUnit.MILLISECONDS); // 10 seconds for first step
        
        // Simulate second step: Prechecks
        scheduler.schedule(() -> {
            String completionTime = LocalTime.now().format(timeFormatter);
            updateStepStatus("2", "completed", 100, "Prechecks completed", "Application connected to MongoDB", completionTime);
            
            // Start third step immediately after second completes
            String startTimeStep3 = LocalTime.now().format(timeFormatter);
            updateStepStatus("3", "in-progress", 0, "Starting CDC capture...", "Application connected to MongoDB", startTimeStep3);
            
        }, 20000, TimeUnit.MILLISECONDS); // 20 seconds total (10s first + 10s second)
        
        // Simulate third step: CDC capture
        scheduler.schedule(() -> {
            String completionTime = LocalTime.now().format(timeFormatter);
            updateStepStatus("3", "completed", 100, "CDC capture completed", "Application connected to MongoDB", completionTime);
            
            // Start fourth step immediately after third completes
            String startTimeStep4 = LocalTime.now().format(timeFormatter);
            updateStepStatus("4", "in-progress", 0, "Starting application switchover...", "Application connected to MongoDB", startTimeStep4);
            
        }, 30000, TimeUnit.MILLISECONDS); // 30 seconds total
        
        // Simulate fourth step: Application switchover
        scheduler.schedule(() -> {
            String completionTime = LocalTime.now().format(timeFormatter);
            updateStepStatus("4", "completed", 100, "Application switchover completed", "Application connected to Couchbase", completionTime);
            simulationRunning = false;
            
        }, 40000, TimeUnit.MILLISECONDS); // 40 seconds total
    }
    
    private void updateStepStatus(String stepId, String status, int progress, String details, String connectionStatus, String timestamp) {
        for (DeltaMigration step : migrationSteps) {
            if (step.getId().equals(stepId)) {
                step.setStatus(status);
                step.setProgress(progress);
                step.setDetails(details);
                step.setConnectionStatus(connectionStatus);
                step.setTime(timestamp); // Update with real timestamp
                break;
            }
        }
        // Send the entire updated list to all connected clients
        messagingTemplate.convertAndSend("/topic/migration-status", migrationSteps);
    }
    
    public void resetMigration() {
        if (scheduler != null) {
            scheduler.shutdown();
            scheduler = Executors.newScheduledThreadPool(1);
        }
        simulationRunning = false;
        initializeMigrationSteps();
        messagingTemplate.convertAndSend("/topic/migration-status", migrationSteps);
    }
    
    @PreDestroy
    public void cleanup() {
        if (scheduler != null) {
            scheduler.shutdown();
        }
    }
}