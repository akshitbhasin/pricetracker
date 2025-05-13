package com.sp.pricing.service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class TestUtils {
    
    public static class Constants {
        public static final String INSTRUMENT_ID = "some-instrument-id";
        public static final int THREAD_COUNT = 10;
        public static final int TIMEOUT_SECONDS = 5;
        public static final double PAYLOAD_DOUBLE = 150.0;
        public static final String PAYLOAD_STRING = "150.0 USD";
        public static final boolean PAYLOAD_BOOLEAN = true;
        public static final long BATCH_ID = 999L;
    }
    
    public static void shutdownExecutor(ExecutorService executor, int timeoutSeconds) throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS);
    }
}
