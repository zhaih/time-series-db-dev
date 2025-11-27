/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.reader.BinaryLabelsStorage;
import org.opensearch.tsdb.core.reader.SortedSetLabelsStorage;

import java.io.IOException;

public class ClosedChunkIndexTSDBDocValuesTests extends OpenSearchTestCase {

    public void testWithBinaryLabels() throws IOException {
        MockBinaryDocValues chunkDocValues = new MockBinaryDocValues(new BytesRef("chunk_data"));
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues(new BytesRef("labels_data"));

        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(
            chunkDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        assertSame("Should return the same chunk doc values", chunkDocValues, tsdbDocValues.getChunkDocValues());
        assertSame("Should return the same labels binary doc values", labelsBinaryDocValues, tsdbDocValues.getLabelsBinaryDocValues());
        assertNull("Should return null for sorted set labels", tsdbDocValues.getLabelsSortedSetDocValues());
        assertEquals("Should have BINARY storage type", LabelStorageType.BINARY, tsdbDocValues.getLabelStorageType());
    }

    public void testWithSortedSetLabels() throws IOException {
        MockBinaryDocValues chunkDocValues = new MockBinaryDocValues(new BytesRef("chunk_data"));
        MockSortedSetDocValues labelsSortedSetDocValues = new MockSortedSetDocValues();

        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(
            chunkDocValues,
            new SortedSetLabelsStorage(labelsSortedSetDocValues)
        );

        assertSame("Should return the same chunk doc values", chunkDocValues, tsdbDocValues.getChunkDocValues());
        assertSame(
            "Should return the same labels sorted set doc values",
            labelsSortedSetDocValues,
            tsdbDocValues.getLabelsSortedSetDocValues()
        );
        assertNull("Should return null for binary labels", tsdbDocValues.getLabelsBinaryDocValues());
        assertEquals("Should have SORTED_SET storage type", LabelStorageType.SORTED_SET, tsdbDocValues.getLabelStorageType());
    }

    public void testGetChunkRefDocValuesThrowsException() throws IOException {
        MockBinaryDocValues chunkDocValues = new MockBinaryDocValues(new BytesRef("chunk_data"));
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues(new BytesRef("labels_data"));

        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(
            chunkDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkRefDocValues);
        assertEquals("Closed Chunk Index does not support chunk references", exception.getMessage());
    }

    public void testWithNullValues() throws IOException {
        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(null, new BinaryLabelsStorage(null));

        assertNull("Should return null chunk doc values", tsdbDocValues.getChunkDocValues());
        assertNull("Should return null labels binary doc values", tsdbDocValues.getLabelsBinaryDocValues());
        assertEquals("Should have BINARY storage type", LabelStorageType.BINARY, tsdbDocValues.getLabelStorageType());

        expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkRefDocValues);
    }

    public void testWithSortedSetLabelsNullValues() throws IOException {
        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(null, new SortedSetLabelsStorage(null));

        assertNull("Should return null chunk doc values", tsdbDocValues.getChunkDocValues());
        assertNull("Should return null labels sorted set doc values", tsdbDocValues.getLabelsSortedSetDocValues());
        assertEquals("Should have SORTED_SET storage type", LabelStorageType.SORTED_SET, tsdbDocValues.getLabelStorageType());

        expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkRefDocValues);
    }

    public void testWithRealChunkValues() throws IOException {
        MockBinaryDocValues chunkDocValues = new MockBinaryDocValues(new BytesRef("chunk_data_123"));
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues(new BytesRef("labels_data_456"));

        ClosedChunkIndexTSDBDocValues tsdbDocValues = ClosedChunkIndexTSDBDocValues.create(
            chunkDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        // Test that we can actually use the doc values
        assertTrue("Should advance to document", chunkDocValues.advanceExact(0));
        assertEquals("Should return correct chunk data", new BytesRef("chunk_data_123"), chunkDocValues.binaryValue());

        assertTrue("Should advance labels binary doc values", labelsBinaryDocValues.advanceExact(0));
        assertEquals("Should return correct labels data", new BytesRef("labels_data_456"), labelsBinaryDocValues.binaryValue());
    }

    static class MockBinaryDocValues extends BinaryDocValues {
        private final BytesRef value;

        MockBinaryDocValues(BytesRef value) {
            this.value = value;
        }

        @Override
        public BytesRef binaryValue() throws IOException {
            return value;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return target == 0;
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    static class MockSortedSetDocValues extends SortedSetDocValues {
        private long ord = 0;

        @Override
        public long nextOrd() throws IOException {
            if (ord == 0) {
                ord++;
                return 0;
            }
            return -1L; // NO_MORE_ORDS
        }

        @Override
        public int docValueCount() {
            return 1;
        }

        @Override
        public BytesRef lookupOrd(long ord) throws IOException {
            return new BytesRef("test_label");
        }

        @Override
        public long getValueCount() {
            return 1;
        }

        @Override
        public int docID() {
            return 0;
        }

        @Override
        public int nextDoc() throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) throws IOException {
            return NO_MORE_DOCS;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return target == 0;
        }

        @Override
        public long cost() {
            return 1;
        }
    }
}
