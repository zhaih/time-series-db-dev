/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import org.opensearch.tsdb.framework.utils.DurationDeserializer;
import org.opensearch.tsdb.framework.utils.TimestampDeserializer;

import java.time.Duration;
import java.time.Instant;

/**
 * Time configuration for input data and queries
 * Timestamps are parsed once during deserialization to ensure consistency
 */
public record TimeConfig(@JsonProperty("min_timestamp") @JsonDeserialize(using = TimestampDeserializer.class) Instant minTimestamp,
    @JsonProperty("max_timestamp") @JsonDeserialize(using = TimestampDeserializer.class) Instant maxTimestamp,
    @JsonProperty("step") @JsonDeserialize(using = DurationDeserializer.class) Duration step) {
}
