package com.sp.pricing;

import com.sp.pricing.domain.Batch;
import com.sp.pricing.exception.BatchUploadException;


public class BatchUtils {
    public static void validateBatchForUpload(Batch batch) {
        if (batch.isCompleted() || batch.isCancelled()) {
            throw new BatchUploadException(String.format("Cannot upload to a batch with status: %s", batch.getStatus()));
        }
    }

    public static String logBatchUploadMessage(long batchId, int recordCount, int chunkSize) {
        return recordCount > chunkSize ?
            String.format("Uploaded %s records to batch %s (exceeds recommended chunk size %s)", recordCount, batchId, chunkSize)
        : String.format("Uploaded %s records to batch %s", recordCount, batchId);
    }
}
