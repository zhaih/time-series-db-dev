/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import org.opensearch.common.unit.TimeValue;

import java.io.IOException;
import java.time.Duration;

/**
 * Custom deserializer for duration strings
 * Parses durations once during deserialization using OpenSearch's TimeValue
 */
public class DurationDeserializer extends JsonDeserializer<Duration> {

    @Override
    public Duration deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        String durationStr = parser.getText();
        if (durationStr == null || durationStr.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration string cannot be null or empty");
        }
        try {
            TimeValue timeValue = TimeValue.parseTimeValue(durationStr.trim(), "duration");
            return Duration.ofMillis(timeValue.getMillis());
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid duration format: " + durationStr + ". Expected format like '30s', '5m', '1h', '2d'",
                e
            );
        }
    }
}
