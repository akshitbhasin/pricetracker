package com.sp.pricing.service;

import com.sp.pricing.domain.Batch;
import com.sp.pricing.domain.BatchStatus;
import com.sp.pricing.domain.PriceRecord;
import com.sp.pricing.exception.BatchCompletionException;
import com.sp.pricing.exception.BatchException;
import com.sp.pricing.exception.BatchNotFoundException;
import com.sp.pricing.exception.BatchUploadException;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.sp.pricing.utils.BatchUtils.logBatchUploadMessage;
import static com.sp.pricing.utils.BatchUtils.validateBatchForUpload;
import static com.sp.pricing.utils.Constants.CHUNK_SIZE_ARGUMENT;
import static com.sp.pricing.utils.Constants.CHUNK_SIZE_DEFAULT_VALUE;

/**
 * In-memory implementation of the PriceService that maintains the latest prices of instruments.
 * This service is thread-safe and suitable for medium to large scale workloads. It supports concurrent producers and consumers,
 * batch uploads with atomic commit, and queries for the latest price as of a given timestamp.
 *
 */
public class PriceServiceImpl implements PriceService {

    private static final Logger logger = LoggerFactory.getLogger(PriceServiceImpl.class);

    /** Configurable batch chunk size (for logging warnings). */
    private final int batchChunkSize;

    /** Map of instrument ID to its price history (sorted by timestamp). Only accessed under the rwLock. */
    private final Map<String, NavigableMap<Instant, PriceRecord<?>>> instrumentPrices;

    /** Map of active (ongoing) batches, allowing thread-safe concurrent access by batch ID. */
    private final ConcurrentHashMap<Long, Batch> activeBatches;

    /** Generator for unique batch IDs. */
    private final AtomicLong nextBatchId;

    /** Read-write lock to control concurrent access to instrumentPrices. Allows multiple readers or one writer. */
    private final ReentrantReadWriteLock readWriteLock;

    /** Default Constructor (uses System Properties for batch size) */
    public PriceServiceImpl() {
        this(
                Integer.parseInt(System.getProperty(CHUNK_SIZE_ARGUMENT, CHUNK_SIZE_DEFAULT_VALUE)),
                new ConcurrentHashMap<>(),
                new ReentrantReadWriteLock()
        );
    }

    /** Fully Configurable Constructor */
    public PriceServiceImpl(int batchChunkSize,
                            ConcurrentHashMap<Long, Batch> activeBatches,
                            ReentrantReadWriteLock readWriteLock) {
        this.batchChunkSize = batchChunkSize;
        this.activeBatches = activeBatches;
        this.instrumentPrices = new HashMap<>(); // Always in-memory
        this.readWriteLock = readWriteLock;
        this.nextBatchId = new AtomicLong(1);
    }

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
        logBatchUploadMessage(batchId, records.size(), batchChunkSize);
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
            readWriteLock.writeLock().lock();
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
                readWriteLock.writeLock().unlock();
            }
        }

        logger.info("Completed batch {} ({} records processed)", batchId, batch.getRecords().size());
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
            batch.getRecords().clear();
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
        readWriteLock.readLock().lock();
        try {
            return Optional.ofNullable(instrumentPrices.get(instrumentId))
                    .filter(map -> !map.isEmpty())
                    .map(map -> map.floorEntry(asOf))
                    .map(Map.Entry::getValue);
        } finally {
            readWriteLock.readLock().unlock();
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