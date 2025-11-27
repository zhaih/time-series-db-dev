/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;

/**
 * Label storage implementation using BinaryDocValues.
 * Provides faster reads/writes with lower memory overhead.
 */
public final class BinaryLabelsStorage implements LabelsStorage {
    private final BinaryDocValues docValues;

    /**
     * Creates a new binary labels storage.
     *
     * @param docValues the binary doc values containing serialized labels
     */
    public BinaryLabelsStorage(BinaryDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public LabelStorageType getStorageType() {
        return LabelStorageType.BINARY;
    }

    @Override
    public Labels readLabels(int docId) throws IOException {
        if (docValues == null || !docValues.advanceExact(docId)) {
            return ByteLabels.emptyLabels();
        }
        BytesRef serialized = docValues.binaryValue();
        // Copy bytes because BytesRef might be reused by Lucene
        byte[] copy = new byte[serialized.length];
        System.arraycopy(serialized.bytes, serialized.offset, copy, 0, serialized.length);
        return ByteLabels.fromRawBytes(copy);
    }

    /**
     * Gets the underlying doc values.
     *
     * @return the binary doc values (may be null if field doesn't exist)
     */
    public BinaryDocValues getDocValues() {
        return docValues;
    }
}
