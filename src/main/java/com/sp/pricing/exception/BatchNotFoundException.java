package com.sp.pricing.exception;

public class BatchNotFoundException extends BatchException{
    public BatchNotFoundException(String message) {
        super(message);
    }

    public BatchNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
