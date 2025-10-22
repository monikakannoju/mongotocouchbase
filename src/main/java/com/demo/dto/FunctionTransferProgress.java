package com.demo.dto;

import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.*;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionTransferProgress {
    @JsonProperty("name")
    private String functionName;
    @JsonProperty("status")
    private String status;
    @JsonProperty("processed")
    private int processed;
    @JsonProperty("total")
    private int total;
    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp;
    @JsonProperty("message")
    private String message;

    @JsonCreator
    public FunctionTransferProgress(
        @JsonProperty("name") String functionName,
        @JsonProperty("status") String status,
        @JsonProperty("processed") int processed,
        @JsonProperty("total") int total,
        @JsonProperty("message") String message) {
        this.functionName = functionName;
        this.status = status;
        this.processed = processed;
        this.total = total;
        this.message = message;
        this.timestamp = LocalDateTime.now();
    }

    // Simplified constructor for common cases
    public FunctionTransferProgress(String status, int processed, int total) {
        this(null, status, processed, total, null);
    }

    @JsonIgnore
    public double getPercentage() {
        return total > 0 ? Math.min(100, (processed * 100.0) / total) : 0;
    }

    @JsonIgnore
    public boolean isCompleted() {
        return "COMPLETED".equalsIgnoreCase(status);
    }

    @JsonIgnore
    public boolean isError() {
        return "ERROR".equalsIgnoreCase(status);
    }

    // Explicit getter for message (though @Data should generate it)
    public String getMessage() {
        return this.message;
    }
}