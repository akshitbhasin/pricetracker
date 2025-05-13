package com.sp.pricing.service;

import com.sp.pricing.domain.PriceRecord;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Interface for an in-memory service that tracks the last prices of financial instruments.
 * <p>
 * Allows multiple producers to batch-upload price records and multiple consumers to query the latest price for an instrument as of a given time.
 *
 */
public interface PriceService {

    /**
     * Starts a new batch upload session.
     *
     * @return a unique batch identifier for the new batch.
     */
    long startBatch();

    /**
     * Uploads a chunk of price records to an active batch. The records remain staged until the batch is completed.
     *
     * @param batchId the identifier of the batch (obtained from {@link #startBatch()}).
     * @param records the list of price records to upload in this chunk.
     * @throws IllegalStateException if the batchId is invalid, or if the batch has already been completed/canceled.
     */
    void uploadBatch(long batchId, List<PriceRecord<?>> records);

    /**
     * Completes the batch upload, making all uploaded records visible to consumers atomically.
     *
     * @param batchId the identifier of the batch to complete.
     * @throws IllegalStateException if the batchId is invalid or if the batch was already completed or canceled.
     */
    void completeBatch(long batchId);

    /**
     * Cancels the batch upload, discarding any uploaded records. No records from this batch will be visible to consumers.
     *
     * @param batchId the identifier of the batch to cancel.
     * @throws IllegalStateException if the batchId is invalid or if the batch was already completed or canceled.
     */
    void cancelBatch(long batchId);

    /**
     * Retrieves the most recent price record for the given instrument as of now.
     *
     * @param instrumentId the instrument identifier.
     * @return an {@link Optional} containing the latest {@link PriceRecord} for the instrument, or empty if no price is available.
     */
    Optional<PriceRecord<?>> getLatestPrice(String instrumentId);

    /**
     * Retrieves the most recent price record for the given instrument as of the specified timestamp.
     * <p>
     * This returns the latest price record whose {@code asOf} time is less than or equal to the provided timestamp.
     *
     * @param instrumentId the instrument identifier.
     * @param asOf the cut-off timestamp for the query.
     * @return an {@link Optional} containing the latest {@link PriceRecord} for the instrument as of that time, or empty if none is available up to that time.
     */
    Optional<PriceRecord<?>> getLatestPrice(String instrumentId, Instant asOf);
}