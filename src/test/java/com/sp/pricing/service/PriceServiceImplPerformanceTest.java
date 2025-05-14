package com.sp.pricing.service;

import com.sp.pricing.domain.PriceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.sp.pricing.service.TestUtils.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class PriceServiceImplPerformanceTest {

    private PriceServiceImpl priceService;

    @BeforeEach
    void setUp() {
        priceService = new PriceServiceImpl();
    }

    @Test
    void shouldBatchUploadPerformantly() {
        long batchId = priceService.startBatch();
        long startTime = System.nanoTime();

        IntStream.range(0, PERFORMANCE_TESTING_RECORD_COUNT)
                .forEachOrdered(i -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE))));

        priceService.completeBatch(batchId);
        long duration = System.nanoTime() - startTime;

        long durationInMs = TimeUnit.NANOSECONDS.toMillis(duration);
        System.out.println("Batch upload completed in " + durationInMs + " ms");

        // Assert that the batch upload and completion is fast.
        assertTrue(durationInMs < PERFORMANCE_TEST_THRESHOLD_TIME_MILLISECONDS, "Batch upload should complete within 500 ms");
    }

    @Test
    void shouldBatchUploadMultiThreadedPerformantly() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(PERFORMANCE_TEST_THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(PERFORMANCE_TESTING_RECORD_COUNT);

        long startTime = System.nanoTime();

        IntStream.range(0, PERFORMANCE_TESTING_RECORD_COUNT).forEach(i -> executor.submit(() -> {
            try {
                priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
            } finally {
                latch.countDown();
            }
        }));

        latch.await(10, TimeUnit.SECONDS);
        priceService.completeBatch(batchId);

        long duration = System.nanoTime() - startTime;
        System.out.println("Multi-threaded batch upload completed in " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms");

        Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice(INSTRUMENT_ID);
        assertTrue(latestPrice.isPresent());
    }

    @Test
    void shouldUploadMultipleBatchesAndConsumePerformantly() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(PERFORMANCE_TEST_THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(PERFORMANCE_TEST_THREAD_COUNT);

        long startTime = System.nanoTime();

        IntStream.range(0, PERFORMANCE_TEST_THREAD_COUNT).forEach(i -> executor.submit(() -> {
            try {
                long batchId = priceService.startBatch();
                priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID + "-" + i, Instant.now(), PAYLOAD_DOUBLE)));
                priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID + "-" +i, Instant.now(), PAYLOAD_STRING)));
                priceService.completeBatch(batchId);
            } finally {
                latch.countDown();
            }
        }));

        IntStream.range(0, PERFORMANCE_TEST_THREAD_COUNT).forEach(i -> executor.submit(() -> {
            try {
                Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice(INSTRUMENT_ID + "-" + i);
                assertTrue(latestPrice.isPresent(), "Price should exist for AAPL-" + i);
                assertEquals(PAYLOAD_STRING, latestPrice.get().payload());
            } finally {
                latch.countDown();
            }
        }));

        latch.await(PERFORMANCE_TEST_THREAD_COUNT, TimeUnit.SECONDS);
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        long duration = System.nanoTime() - startTime;

        //Using heavy load variable as the test is configurable and can take some time to cover in case of multiple batches and uploads.
        System.out.println("Concurrent multiple batch creation and consumption completed in " + TimeUnit.NANOSECONDS.toMillis(duration) + " ms");
        assertTrue(TimeUnit.NANOSECONDS.toMillis(duration) < HEAVY_LOAD_PERFORMANCE_TEST_THRESHOLD_TIME_MILLISECONDS, "Concurrent multiple batch creation and consumption should complete within 2000 ms");

    }
}