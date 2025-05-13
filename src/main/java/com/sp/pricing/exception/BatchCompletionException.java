package com.sp.pricing.exception;

public class BatchCompletionException extends BatchException{
    public BatchCompletionException(String message) {
        super(message);
    }

    public BatchCompletionException(String message, Throwable cause) {
        super(message, cause);
    }
}
