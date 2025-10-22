package com.demo.exception;

public class DatabaseTransferException extends RuntimeException {
    public DatabaseTransferException(String message) {
        super(message);
    }
    
    public DatabaseTransferException(String message, Throwable cause) {
        super(message, cause);
    }
}
