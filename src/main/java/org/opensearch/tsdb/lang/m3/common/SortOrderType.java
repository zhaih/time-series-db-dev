/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

public enum SortOrderType {
    /**
     * Ascending order
     */
    ASC("asc"),

    /**
     * Descending order
     */
    DESC("desc");

    private final String value;

    SortOrderType(String value) {
        this.value = value;
    }

    /**
     * Gets the string value for this sort order type.
     * @return The lowercase string value (e.g., "asc", "desc")
     */
    public String getValue() {
        return value;
    }

    public static SortOrderType fromString(String sortOrderType) {
        switch (sortOrderType) {
            case Constants.Functions.Sort.ASC, Constants.Functions.Sort.ASCENDING:
                return ASC;
            case Constants.Functions.Sort.DESC, Constants.Functions.Sort.DESCENDING:
                return DESC;
            default:
                throw new IllegalArgumentException(
                    "Invalid sort order type: " + sortOrderType + ", Supported: asc, ascending, desc, descending"
                );
        }
    }
}
