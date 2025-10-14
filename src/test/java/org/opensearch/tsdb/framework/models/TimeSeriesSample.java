/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import java.time.Instant;
import java.util.Map;

/**
 * Time series sample data for testing
 */
public record TimeSeriesSample(Instant timestamp, double value, Map<String, String> labels) {
}
