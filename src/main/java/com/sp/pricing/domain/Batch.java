package com.sp.pricing.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * A batch of price records being uploaded as part of a batch operation.
 *
 */
public class Batch {

    private final long id;
    private final List<PriceRecord<?>> records;
    private BatchStatus status;

    private Batch(long id, List<PriceRecord<?>> records, BatchStatus status) {
        this.id = id;
        this.records = new ArrayList<>(records); // Defensive copy for immutability
        this.status = status;
    }

    /**
     * Gets the ID of the batch.
     *
     * @return the unique ID of the batch
     */
    public long getId() {
        return id;
    }

    /**
     * Gets an unmodifiable list of records in the batch.
     *
     * @return the list of price records
     */
    public List<PriceRecord<?>> getRecords() {
        return records;
    }

    public BatchStatus getStatus() {
        return status;
    }

    public void setStatus(BatchStatus status){
        this.status = status;
    }

    /**
     * Checks if the batch is in a startable state.
     */
    public boolean isStartable() {
        return this.status == BatchStatus.STARTED;
    }

    /**
     * Checks if the batch is currently uploading.
     */
    public boolean isUploading() {
        return this.status == BatchStatus.UPLOADING || this.status == BatchStatus.STARTED;
    }

    /**
     * Checks if the batch is completed.
     */
    public boolean isCompleted() {
        return this.status == BatchStatus.COMPLETED;
    }

    /**
     * Checks if the batch is cancelled.
     */
    public boolean isCancelled() {
        return this.status == BatchStatus.CANCELLED;
    }

    /**
     * Provides a static method to create a new Builder for a Batch.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class for creating instances of {@link Batch}.
     *
     */
    public static class Builder {
        private long id;
        private List<PriceRecord<?>> records = new ArrayList<>();
        private BatchStatus status;

        /**
         * Sets the ID of the batch.
         *
         * @param id the batch ID
         * @return the builder instance
         */
        public Builder withId(long id) {
            this.id = id;
            return this;
        }

        /**
         * Sets the current status of the batch.
         *
         * @param status the batch Status
         * @return the builder instance
         */
        public Builder withStatus(BatchStatus status) {
            this.status = status;
            return this;
        }

        /**
         * Adds a single price record to the batch.
         * @param record the price record to add
         * @return the builder instance
         */
        public Builder addRecord(PriceRecord<?> record) {
            if (record != null) {
                this.records.add(record);
            }
            return this;
        }

        /**
         * Adds multiple price records to the batch.
         *
         * @param records the list of price records to add
         * @return the builder instance
         */
        public Builder addRecords(List<PriceRecord<?>> records) {
            if (records != null) {
                this.records.addAll(records);
            }
            return this;
        }

        /**
         * Builds the final Batch instance.
         *
         * @return a new {@link Batch} instance with the configured properties
         */
        public Batch build() {
            return new Batch(id, records, status);
        }
    }
}