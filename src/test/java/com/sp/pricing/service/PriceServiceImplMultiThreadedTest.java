package com.sp.pricing.service;


import com.sp.pricing.domain.PriceRecord;
import com.sp.pricing.exception.BatchNotFoundException;
import com.sp.pricing.exception.BatchUploadException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.sp.pricing.service.TestUtils.Constants.*;
import static com.sp.pricing.service.TestUtils.shutdownExecutor;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PriceServiceImplMultiThreadedTest {

    private PriceServiceImpl priceService;

    @BeforeEach
    void setUp() {
        priceService = new PriceServiceImpl();
    }

    @Test
    void shouldConcurrentUploadWithDifferentDataTypes() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        executor.submit(() -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE))));
        executor.submit(() -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_STRING))));
        executor.submit(() -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_BOOLEAN))));

        latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        priceService.completeBatch(batchId);

        shutdownExecutor(executor, TIMEOUT_SECONDS);

        Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice(INSTRUMENT_ID);
        assertTrue(latestPrice.isPresent());
    }

    @Test
    void shouldReturnExpectedResponseWhenEncounteredRaceConditionOnBatchCompletion() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        executor.submit(() -> priceService.completeBatch(batchId));
        executor.submit(() -> priceService.completeBatch(batchId));

        shutdownExecutor(executor, TIMEOUT_SECONDS);

        Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice("AAPL");
        assertTrue(latestPrice.isEmpty(), "No price should exist after race condition on completion");
    }

    @Test
    void shouldEncounterExceptionWhenBatchUploadFails() throws InterruptedException {
        long batchId = priceService.startBatch();

        assertThrows(BatchUploadException.class, () -> {
            priceService.uploadBatch(batchId, null);
        });

        assertThrows(BatchUploadException.class, () -> {
            priceService.uploadBatch(batchId, List.of());
        });
    }

    @Test
    void shouldPerformConcurrentBatchUploadsSuccessfullyInSingleBatch() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        producePriceRecordsInSingleBatchConcurrently(executor, batchId, latch);

        shutdownExecutor(executor, TIMEOUT_SECONDS);

        latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        priceService.completeBatch(batchId);

        Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice(INSTRUMENT_ID);
        assertTrue(latestPrice.isPresent(), "Latest price should be present after concurrent uploads");
    }

    @Test
    void shouldReturnEmptyResponseWhenBatchCompleteBatchIsCancelled() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        executor.submit(() -> priceService.completeBatch(batchId));
        executor.submit(() -> priceService.cancelBatch(batchId));
        executor.submit(() -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE))));

        shutdownExecutor(executor, TIMEOUT_SECONDS);

        Optional<PriceRecord<?>> latestPrice = priceService.getLatestPrice(INSTRUMENT_ID);
        assertTrue(latestPrice.isEmpty(), "No price should exist after cancel");
    }

    @Test
    void shouldCancelBatchBySomeThreadWhenMultipleThreadsTryToCancel() throws InterruptedException {
        long batchId = priceService.startBatch();
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        cancelBatchConcurrently(executor, batchId, latch);

        latch.await(5, TimeUnit.SECONDS);

        // Ensure the batch is canceled
        assertThrows(BatchNotFoundException.class, () -> priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE))));
        assertThrows(BatchNotFoundException.class, () -> priceService.completeBatch(batchId));
    }



    @Test
    void shouldProduceAndConsumeInMultipleBatchesConcurrently() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        producePriceRecordsInMultipleBatchesConcurrently(executor, latch);

        shutdownExecutor(executor, TIMEOUT_SECONDS);

        latch.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ConsumeLatestPricesConcurrently();
    }

    private void producePriceRecordsInMultipleBatchesConcurrently(ExecutorService executor, CountDownLatch latch) {
        IntStream.range(0, THREAD_COUNT)
                .forEach(index -> executor.submit(() -> {
                    try {
                        long batchId = priceService.startBatch();
                        priceService.uploadBatch(batchId,
                                Collections.singletonList(new PriceRecord<>(INSTRUMENT_ID + "-" + index, Instant.now(), PAYLOAD_DOUBLE)));
                        priceService.completeBatch(batchId);
                    } finally {
                        latch.countDown();
                    }
                }));
    }

    private void producePriceRecordsInSingleBatchConcurrently(ExecutorService executor, long batchId, CountDownLatch latch) {
        IntStream.range(0, THREAD_COUNT).forEach(index ->
                executor.submit(() -> {
                    try {
                        priceService.uploadBatch(batchId,
                                Collections.singletonList(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
                    } finally {
                        latch.countDown();
                    }
                })
        );
    }

    private void ConsumeLatestPricesConcurrently() {
        IntStream.range(0, THREAD_COUNT)
                .mapToObj(i -> priceService.getLatestPrice(INSTRUMENT_ID + "-" + i))
                .forEach(latestPrice ->
                        assertTrue(latestPrice.isPresent(),
                                "Price should exist for " + INSTRUMENT_ID + "-" + latestPrice.get().instrumentId())
                );
    }

    private void cancelBatchConcurrently(ExecutorService executor, long batchId, CountDownLatch latch) {
        IntStream.range(0, THREAD_COUNT).forEach(i -> executor.submit(() -> {
            try {
                priceService.cancelBatch(batchId);
            } finally {
                latch.countDown();
            }
        }));
    }

}
