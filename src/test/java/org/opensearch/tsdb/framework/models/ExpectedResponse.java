/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Expected response configuration for time series testing
 */
public record ExpectedResponse(@JsonProperty("status") String status, @JsonProperty("data") List<ExpectedData> data,
    @JsonProperty("error_message") String errorMessage) {
}
