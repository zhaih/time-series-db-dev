/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.opensearch.tsdb.framework.translators.QueryType;

/**
 * Query configuration for time series testing
 */
public record QueryConfig(@JsonProperty("name") String name, @JsonProperty("type") QueryType type, @JsonProperty("query") String query,
    @JsonProperty("time_config") TimeConfig config, @JsonProperty("indices") String indices,
    @JsonProperty("expected") ExpectedResponse expected) {
}
