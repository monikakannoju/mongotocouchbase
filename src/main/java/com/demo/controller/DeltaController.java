package com.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import com.demo.service.DeltaService;
import com.demo.dto.DeltaMigration;
import java.util.List;

@Controller
public class DeltaController {
    
    @Autowired
    private DeltaService migrationService;
    
    // REST endpoint to get initial migration status
    @GetMapping("/api/migration/status")
    @ResponseBody
    public List<DeltaMigration> getInitialMigrationStatus() {
        return migrationService.getMigrationStatus();
    }
    
    // WebSocket endpoint to start migration
    @MessageMapping("/start-migration")
    @SendTo("/topic/migration-status")
    public List<DeltaMigration> startMigration() {
        migrationService.startMigrationSimulation();
        return migrationService.getMigrationStatus();
    }
    
    // WebSocket endpoint to reset migration
    @MessageMapping("/reset-migration")
    @SendTo("/topic/migration-status")
    public List<DeltaMigration> resetMigration() {
        migrationService.resetMigration();
        return migrationService.getMigrationStatus();
    }
    
    // Serve the HTML page
    @GetMapping("/")
    public String index() {
        return "index";
    }
}