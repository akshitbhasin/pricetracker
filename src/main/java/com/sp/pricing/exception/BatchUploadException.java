package com.sp.pricing.exception;

public class BatchUploadException extends BatchException{
    public BatchUploadException(String message) {
        super(message);
    }

    public BatchUploadException(String message, Throwable cause) {
        super(message, cause);
    }
}
