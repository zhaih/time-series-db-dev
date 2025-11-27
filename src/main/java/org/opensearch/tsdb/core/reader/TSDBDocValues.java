/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.tsdb.core.mapping.LabelStorageType;

/**
 * A wrapper class for holding different DocValues types for time series index so that DocValues can be used in same thread that acquired them.
 *
 * <p>Uses composition with {@link LabelsStorage} for label storage, allowing different
 * storage strategies (binary or sorted set) without exposing implementation details.</p>
 *
 * <p>Use static factory methods in subclasses to create instances based on storage type.</p>
 */
public abstract class TSDBDocValues {
    /** The numeric doc values containing chunk references */
    protected NumericDocValues chunkRefDocValues;
    /** The binary doc values containing serialized chunk data */
    protected BinaryDocValues chunkDocValues;
    /** The label storage (handles its own read logic) */
    private final LabelsStorage labelsStorage;

    /**
     * Protected constructor - use static factory methods in subclasses to create instances.
     *
     * @param chunkRefDocValues the numeric doc values containing chunk references (null for closed index)
     * @param chunkDocValues the binary doc values containing chunk data (null for live index)
     * @param labelsStorage the label storage for reading labels
     */
    protected TSDBDocValues(NumericDocValues chunkRefDocValues, BinaryDocValues chunkDocValues, LabelsStorage labelsStorage) {
        this.chunkRefDocValues = chunkRefDocValues;
        this.chunkDocValues = chunkDocValues;
        this.labelsStorage = labelsStorage;
    }

    /**
     * Gets the numeric doc values containing chunk references.
     *
     * @return the chunk reference doc values
     */
    public abstract NumericDocValues getChunkRefDocValues();

    /**
     * Gets the binary doc values containing serialized chunk data.
     *
     * @return the chunk doc values
     */
    public abstract BinaryDocValues getChunkDocValues();

    /**
     * Gets the binary doc values containing serialized labels.
     * This is a convenience method that checks the storage type.
     *
     * @return the labels binary doc values (null if using sorted_set storage)
     */
    public BinaryDocValues getLabelsBinaryDocValues() {
        if (labelsStorage instanceof BinaryLabelsStorage binaryStorage) {
            return binaryStorage.getDocValues();
        }
        return null;
    }

    /**
     * Gets the sorted set doc values containing labels.
     * This is a convenience method that checks the storage type.
     *
     * @return the labels sorted set doc values (null if using binary storage)
     */
    public SortedSetDocValues getLabelsSortedSetDocValues() {
        if (labelsStorage instanceof SortedSetLabelsStorage sortedSetStorage) {
            return sortedSetStorage.getDocValues();
        }
        return null;
    }

    /**
     * Gets the label storage type being used.
     *
     * @return the label storage type (BINARY or SORTED_SET)
     */
    public LabelStorageType getLabelStorageType() {
        return labelsStorage.getStorageType();
    }

    /**
     * Gets the label storage for reading labels.
     *
     * @return the labels storage implementation
     */
    public LabelsStorage getLabelsStorage() {
        return labelsStorage;
    }
}
