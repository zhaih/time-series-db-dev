/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.TSDBDocValues;

/**
 * LiveSeriesIndexTSDBDocValues is a wrapper class for holding chunk reference doc values and labels doc values for live series index.
 * Use static factory methods to create instances.
 */
public class LiveSeriesIndexTSDBDocValues extends TSDBDocValues {

    /**
     * Private constructor - use static factory methods.
     */
    private LiveSeriesIndexTSDBDocValues(NumericDocValues chunkRefDocValues, LabelsStorage labelsStorage) {
        super(chunkRefDocValues, null, labelsStorage);
    }

    /**
     * Creates a LiveSeriesIndexTSDBDocValues with the specified label storage.
     *
     * @param chunkRefDocValues the numeric doc values containing chunk references
     * @param labelsStorage the labels storage (binary or sorted_set)
     * @return a new instance configured with the provided label storage
     */
    public static LiveSeriesIndexTSDBDocValues create(NumericDocValues chunkRefDocValues, LabelsStorage labelsStorage) {
        return new LiveSeriesIndexTSDBDocValues(chunkRefDocValues, labelsStorage);
    }

    @Override
    public NumericDocValues getChunkRefDocValues() {
        return this.chunkRefDocValues;
    }

    @Override
    public BinaryDocValues getChunkDocValues() {
        throw new UnsupportedOperationException("Live Series Index does not support chunk doc values");
    }
}
