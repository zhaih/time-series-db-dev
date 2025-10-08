/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Enumeration representing the types of value filters that can be applied.
 */
public enum ValueFilterType {

    /**
     * Equal to
     */
    EQ,

    /**
     * Not equal to
     */
    NEQ,

    /**
     * Greater than
     */
    GT,

    /**
     * Greater than or equal to
     */
    GTE,

    /**
     * Less than
     */
    LT,

    /**
     * Less than or equal to
     */
    LTE
}
