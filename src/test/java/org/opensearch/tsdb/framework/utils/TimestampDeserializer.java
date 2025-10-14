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

import org.opensearch.tsdb.framework.YamlLoader;
import org.opensearch.tsdb.utils.TimestampUtils;

import java.io.IOException;
import java.time.Instant;

/**
 * Custom deserializer for timestamp strings.
 * Uses a shared reference time from the DeserializationContext to ensure all
 * relative timestamps (e.g., "now-50m") within a single YAML file use the same
 * base time, preventing test flakiness from time drift between deserializations.
 */
public class TimestampDeserializer extends JsonDeserializer<Instant> {

    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        String timestampStr = parser.getText();

        // Try to get the reference time from the context
        Object referenceTimeObj = context.getAttribute(YamlLoader.REFERENCE_TIME_KEY);

        if (referenceTimeObj instanceof Instant) {
            // Use the shared reference time for consistent "now" across all timestamps
            return TimestampUtils.parseTimestampRelativeTo(timestampStr, (Instant) referenceTimeObj);
        } else {
            // Fallback to current time if no reference time is set
            return TimestampUtils.parseTimestamp(timestampStr);
        }
    }
}
