/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Label storage implementation using SortedSetDocValues.
 * Provides better compression for high cardinality labels and efficient range queries.
 */
public final class SortedSetLabelsStorage implements LabelsStorage {
    private final SortedSetDocValues docValues;

    /**
     * Creates a new sorted set labels storage.
     *
     * @param docValues the sorted set doc values containing labels
     */
    public SortedSetLabelsStorage(SortedSetDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public LabelStorageType getStorageType() {
        return LabelStorageType.SORTED_SET;
    }

    @Override
    public Labels readLabels(int docId) throws IOException {
        if (docValues == null || !docValues.advanceExact(docId)) {
            return ByteLabels.emptyLabels();
        }

        int valueCount = docValues.docValueCount();
        List<String> labelStrings = new ArrayList<>(valueCount);

        for (int i = 0; i < valueCount; i++) {
            long ord = docValues.nextOrd();
            BytesRef term = docValues.lookupOrd(ord);
            String labelKVString = term.utf8ToString();
            labelStrings.add(labelKVString);
        }

        return ByteLabels.fromSortedKeyValuePairs(labelStrings);
    }

    /**
     * Gets the underlying doc values.
     *
     * @return the sorted set doc values (may be null if field doesn't exist)
     */
    public SortedSetDocValues getDocValues() {
        return docValues;
    }
}
