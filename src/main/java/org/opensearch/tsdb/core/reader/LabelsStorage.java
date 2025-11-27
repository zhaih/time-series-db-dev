/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;

/**
 * Interface for label storage strategies in TSDB.
 * Implementations handle reading labels from different Lucene DocValues types.
 *
 * <p>Each implementation encapsulates its own deserialization logic, eliminating
 * the need for type checks at call sites.</p>
 */
public interface LabelsStorage {
    /**
     * Gets the storage type being used.
     *
     * @return the label storage type (BINARY or SORTED_SET)
     */
    LabelStorageType getStorageType();

    /**
     * Reads labels for a specific document.
     *
     * @param docId the document ID to read labels for
     * @return the deserialized Labels object, or empty labels if not found
     * @throws IOException if an I/O error occurs
     */
    Labels readLabels(int docId) throws IOException;
}
