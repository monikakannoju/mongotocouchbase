package com.demo.service;
 
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
 
import org.springframework.stereotype.Service;
 
@Service
public class CheckpointService {
    private final Map<String, Checkpoint> checkpointStore = new ConcurrentHashMap<>();
 
    public void saveCheckpoint(Checkpoint checkpoint) {
        checkpointStore.put(checkpoint.getCheckpointId(), checkpoint);
        System.out.println("Saved checkpoint: " + checkpoint.getCheckpointId());
    }
 
    public Checkpoint loadCheckpoint(String checkpointId) {
        return checkpointStore.get(checkpointId);
    }
 
    public void deleteCheckpoint(String checkpointId) {
        checkpointStore.remove(checkpointId);
        System.out.println("Deleted checkpoint: " + checkpointId);
    }
}