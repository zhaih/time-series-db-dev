/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

/**
 * ClosedChunkIndexTSDBDocValues is a wrapper class for holding chunk doc values and labels doc values for closed chunk index.
 *
 */
public class ClosedChunkIndexTSDBDocValues extends TSDBDocValues {

    /**
     * Private constructor - use static factory methods.
     */
    private ClosedChunkIndexTSDBDocValues(BinaryDocValues chunkDocValues, LabelsStorage labelsStorage) {
        super(null, chunkDocValues, labelsStorage);
    }

    /**
     * Creates a ClosedChunkIndexTSDBDocValues with the specified label storage.
     *
     * @param chunkDocValues the binary doc values containing serialized chunk data
     * @param labelsStorage the labels storage (binary or sorted_set)
     * @return a new instance configured with the provided label storage
     */
    public static ClosedChunkIndexTSDBDocValues create(BinaryDocValues chunkDocValues, LabelsStorage labelsStorage) {
        return new ClosedChunkIndexTSDBDocValues(chunkDocValues, labelsStorage);
    }

    @Override
    public NumericDocValues getChunkRefDocValues() {
        throw new UnsupportedOperationException("Closed Chunk Index does not support chunk references");
    }

    @Override
    public BinaryDocValues getChunkDocValues() {
        return this.chunkDocValues;
    }
}
