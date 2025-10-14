/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

/**
 * Input data type enumeration
 */
public enum InputDataType {
    /**
     * Fixed interval metric data type represents metric data with fixed-interval timestamps
     * @FixedIntervalMetricData can be used to represent the data for this data type
     */
    FIXED_INTERVAL,
    /**
     * Generic data type represents metric data with explicit timestamp-value pairs
     * @MetricData can be used to represent the data for this data type
     */
    GENERIC
}
