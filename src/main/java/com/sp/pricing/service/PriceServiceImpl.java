package com.sp.pricing.service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sp.pricing.domain.Batch;
import com.sp.pricing.domain.BatchStatus;
import com.sp.pricing.domain.PriceRecord;
import com.sp.pricing.exception.BatchCompletionException;
import com.sp.pricing.exception.BatchException;
import com.sp.pricing.exception.BatchNotFoundException;
import com.sp.pricing.exception.BatchUploadException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sp.pricing.BatchUtils.logBatchUploadMessage;
import static com.sp.pricing.BatchUtils.validateBatchForUpload;

/**
 * In-memory implementation of the PriceService that maintains the latest prices of instruments.
 * This service is thread-safe and suitable for medium to large scale workloads. It supports concurrent producers and consumers,
 * batch uploads with atomic commit, and queries for the latest price as of a given timestamp.
 *
 */
public class PriceServiceImpl implements PriceService {

    private static final Logger logger = LoggerFactory.getLogger(PriceServiceImpl.class);

    /** Configurable batch chunk size (for logging warnings) via system property "priceService.batch.chunkSize". Default is 1000. */
    private static final int BATCH_CHUNK_SIZE = Integer.parseInt(
            System.getProperty("priceService.batch.chunkSize", "1000"));

    /** Map of instrument ID to its price history (sorted by timestamp). Only accessed under the rwLock. */
    private final Map<String, NavigableMap<Instant, PriceRecord<?>>> instrumentPrices = new HashMap<>();

    /** Map of active (ongoing) batches, allowing thread-safe concurrent access by batch ID. */
    private final ConcurrentHashMap<Long, Batch> activeBatches = new ConcurrentHashMap<>();

    /** Generator for unique batch IDs. */
    private final AtomicLong nextBatchId = new AtomicLong(1);

    /** Read-write lock to control concurrent access to instrumentPrices. Allows multiple readers or one writer. */
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    @Override
    public long startBatch() {
        long batchId = nextBatchId.getAndIncrement();
        Batch batch = Batch.builder()
                .withId(batchId)
                .withStatus(BatchStatus.STARTED)
                .build();
        activeBatches.put(batchId, batch);
        logger.info("Started batch {}", batchId);
        return batchId;
    }

    @Override
    public void uploadBatch(long batchId, List<PriceRecord<?>> records) {
        Optional.ofNullable(records)
                .filter(CollectionUtils::isNotEmpty)
                .orElseThrow(() -> new BatchUploadException("Cannot upload an empty or null list of records"));
        Batch batch = getOrThrow(batchId);
        synchronized (batch) {
            validateBatchForUpload(batch);
            // Transition to UPLOADING status if just started
            if (batch.isStartable()) {
                batch.setStatus(BatchStatus.UPLOADING);
            }
            // Add records to the batch
            batch.getRecords().addAll(records);
        }
        logBatchUploadMessage(batchId, records.size(), BATCH_CHUNK_SIZE);
    }

    @Override
    public void completeBatch(long batchId) {
        // Remove the batch from active list to prevent further modifications during commit
        Batch batch = removeOrThrow(batchId);
        // Synchronize on the batch to ensure no concurrent uploads are happening
        synchronized (batch) {
            if (batch.isCompleted() || batch.isCancelled()) {
                throw new BatchCompletionException("Cannot complete an already completed or canceled batch");
            }
            batch.setStatus(BatchStatus.COMPLETED);

            // Acquire write lock to block readers and other writers while applying updates
            rwLock.writeLock().lock();
            try {
                // Stream-based refactor for updating instrument prices
                batch.getRecords().stream()
                        .forEach(record -> {
                            instrumentPrices
                                    .computeIfAbsent(record.instrumentId(), k -> new TreeMap<>())
                                    .merge(record.asOf(), record, (existing, newRecord) -> {
                                        // Determine if the new record should replace the existing one
                                        return (existing.asOf().isBefore(newRecord.asOf())) ? newRecord : existing;
                                    });
                        });
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        logger.info("Completed batch {} ({} records applied)", batchId, batch.getRecords().size());
        // All records are now visible to queries. The Batch object will be GC'd since it's no longer referenced.
    }

    @Override
    public void cancelBatch(long batchId) {
        Batch batch = removeOrThrow(batchId);
        // Lock the batch to ensure no uploads modify it while we cancel
        synchronized (batch) {
            if (batch.isCompleted()) {
                throw new BatchException("Cannot cancel a completed batch");
            }
            batch.getRecords().clear(); // discard any staged records
        }
        logger.info("Canceled batch: {}", batchId);
    }

    @Override
    public Optional<PriceRecord<?>> getLatestPrice(String instrumentId) {
        // Delegate to the timestamp-based query using current time
        return getLatestPrice(instrumentId, Instant.now());
    }

    @Override
    public Optional<PriceRecord<?>> getLatestPrice(String instrumentId, Instant asOf) {
        rwLock.readLock().lock();
        try {
            return Optional.ofNullable(instrumentPrices.get(instrumentId))
                    .filter(map -> !map.isEmpty())
                    .map(map -> map.floorEntry(asOf))
                    .map(Map.Entry::getValue);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private Batch getOrThrow(long batchId) {
        return Optional.ofNullable(activeBatches.get(batchId))
                .orElseThrow(() -> {
                    String errorMessage = String.format("Batch: %s not found or already completed/canceled", batchId);
                    logger.warn(errorMessage);
                    return new BatchNotFoundException(errorMessage);
                });
    }

    private Batch removeOrThrow(long batchId) {
        return Optional.ofNullable(activeBatches.remove(batchId))
                .orElseThrow(() -> {
                    String errorMessage = String.format("Batch: %s not found or already completed/canceled", batchId);
                    logger.warn(errorMessage);
                    return new BatchNotFoundException(errorMessage);
                });
    }

}