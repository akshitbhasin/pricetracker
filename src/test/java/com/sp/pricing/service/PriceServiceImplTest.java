package com.sp.pricing.service;

import com.sp.pricing.domain.PriceRecord;
import com.sp.pricing.exception.BatchException;
import com.sp.pricing.exception.BatchNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static com.sp.pricing.service.TestUtils.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

public class PriceServiceImplTest {

    private PriceServiceImpl priceService;

    @BeforeEach
    void setUp() {
        priceService = new PriceServiceImpl();
    }

    @Test
    void shouldStartBatchSuccessfully() {
        long batchId = priceService.startBatch();
        assertTrue(batchId > 0);
    }

    @Test
    void shouldThrowIfUploadToNonExistentBatch() {
        assertThrows(BatchNotFoundException.class, () -> {
            priceService.uploadBatch(BATCH_ID, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
        });
    }

    @Test
    void shouldUploadBatchSuccessfully() {
        long batchId = priceService.startBatch();
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
    }

    @Test
    void shouldCompleteBatchSuccessfully() {
        long batchId = priceService.startBatch();
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
        priceService.completeBatch(batchId);
    }

    @Test
    void shouldThrowIfCompleteNonExistentBatch() {
        assertThrows(BatchNotFoundException.class, () -> priceService.completeBatch(BATCH_ID));
    }

    @Test
    void shouldCancelBatchSuccessfully() {
        long batchId = priceService.startBatch();
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
        priceService.cancelBatch(batchId);
    }

    @Test
    void shouldThrowIfCancelBatchUnsuccessful() {
        assertThrows(BatchNotFoundException.class, () -> priceService.cancelBatch(BATCH_ID));
    }

    @Test
    void shouldThrowExceptionIfCancelCompletedBatchIsCancelled() throws InterruptedException {
        long batchId = priceService.startBatch();
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, Instant.now(), PAYLOAD_DOUBLE)));
        priceService.completeBatch(batchId);

        assertThrows(BatchException.class, () -> priceService.cancelBatch(batchId),
                "Expected BatchException when canceling a completed batch");
    }

    @Test
    void shouldGetLatestPrice() throws InterruptedException {
        long batchId = priceService.startBatch();
        Instant early = Instant.now();
        Thread.sleep(2000);
        Instant late = Instant.now();
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, late, PAYLOAD_DOUBLE)));
        priceService.uploadBatch(batchId, List.of(new PriceRecord<>(INSTRUMENT_ID, early, "CANCELLED")));
        priceService.completeBatch(batchId);

        Optional<PriceRecord<?>> price = priceService.getLatestPrice(INSTRUMENT_ID);
        assertTrue(price.isPresent());
        assertEquals(PAYLOAD_DOUBLE, price.get().payload());
    }
}
