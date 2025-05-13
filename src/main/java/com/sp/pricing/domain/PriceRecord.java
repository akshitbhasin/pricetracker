package com.sp.pricing.domain;

import java.time.Instant;

/**
 * A record representing a price data point for a financial instrument.
 *
 * @param <T> the type of the price payload (e.g., a numeric price or a complex object with pricing details)
 */
public record PriceRecord<T>(String instrumentId, Instant asOf, T payload) {
    // The record automatically provides a constructor and accessors (instrumentId(), asOf(), payload()).
}
