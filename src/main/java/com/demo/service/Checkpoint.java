package com.demo.service;
 
import java.util.HashSet;
import java.util.Set;
 
public class Checkpoint {
    private final String checkpointId;
    private final String stage;
    private final int totalSucceeded;
    private final int totalFailed;
    private final int autoTransferred;
    private final int manualRequired;
    private final Set<String> processedFunctionNames;
    private final Set<String> attemptedFunctionNames;
    private final String lastProcessedId;
 
    public Checkpoint(String checkpointId, String stage, int totalSucceeded, int totalFailed, 
                     int autoTransferred, int manualRequired, 
                     Set<String> processedFunctionNames, Set<String> attemptedFunctionNames,
                     String lastProcessedId) {
        this.checkpointId = checkpointId;
        this.stage = stage;
        this.totalSucceeded = totalSucceeded;
        this.totalFailed = totalFailed;
        this.autoTransferred = autoTransferred;
        this.manualRequired = manualRequired;
        this.processedFunctionNames = processedFunctionNames;
        this.attemptedFunctionNames = attemptedFunctionNames;
        this.lastProcessedId = lastProcessedId;
    }
 
    public String getCheckpointId() { return checkpointId; }
    public String getStage() { return stage; }
    public int getTotalSucceeded() { return totalSucceeded; }
    public int getTotalFailed() { return totalFailed; }
    public int getAutoTransferred() { return autoTransferred; }
    public int getManualRequired() { return manualRequired; }
    public Set<String> getProcessedFunctionNames() { return processedFunctionNames; }
    public Set<String> getAttemptedFunctionNames() { return attemptedFunctionNames; }
    public String getLastProcessedId() { return lastProcessedId; }
 
    @Override
    public String toString() {
        return "Checkpoint{" +
                "checkpointId='" + checkpointId + '\'' +
                ", stage='" + stage + '\'' +
                ", totalSucceeded=" + totalSucceeded +
                ", totalFailed=" + totalFailed +
                ", autoTransferred=" + autoTransferred +
                ", manualRequired=" + manualRequired +
                ", processedFunctionNames=" + processedFunctionNames +
                ", attemptedFunctionNames=" + attemptedFunctionNames +
                ", lastProcessedId='" + lastProcessedId + '\'' +
                '}';
    }
}