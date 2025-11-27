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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.reader.BinaryLabelsStorage;
import org.opensearch.tsdb.core.reader.SortedSetLabelsStorage;

import java.io.IOException;

public class LiveSeriesIndexTSDBDocValuesTests extends OpenSearchTestCase {

    public void testConstructorAndBasicMethods() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(
            chunkRefDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        assertSame("Should return the same chunk ref doc values", chunkRefDocValues, tsdbDocValues.getChunkRefDocValues());
        assertSame("Should return the same labels binary doc values", labelsBinaryDocValues, tsdbDocValues.getLabelsBinaryDocValues());
    }

    public void testGetChunkDocValuesThrowsException() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(
            chunkRefDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
        assertEquals("Live Series Index does not support chunk doc values", exception.getMessage());
    }

    public void testWithNullValues() throws IOException {
        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(null, new BinaryLabelsStorage(null));

        assertNull("Should return null chunk ref doc values", tsdbDocValues.getChunkRefDocValues());
        assertNull("Should return null labels binary doc values", tsdbDocValues.getLabelsBinaryDocValues());

        expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
    }

    public void testWithRealValues() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        chunkRefDocValues.setValue(123L);

        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(
            chunkRefDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        // Test that we can actually use the doc values
        assertTrue("Should advance to document", chunkRefDocValues.advanceExact(0));
        assertEquals("Should return correct chunk reference", 123L, chunkRefDocValues.longValue());

        assertTrue("Should advance labels binary doc values", labelsBinaryDocValues.advanceExact(0));
    }

    public void testWithSortedSetLabels() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockSortedSetDocValues labelsSortedSetDocValues = new MockSortedSetDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(
            chunkRefDocValues,
            new SortedSetLabelsStorage(labelsSortedSetDocValues)
        );

        assertSame("Should return the same chunk ref doc values", chunkRefDocValues, tsdbDocValues.getChunkRefDocValues());
        assertSame(
            "Should return the same labels sorted set doc values",
            labelsSortedSetDocValues,
            tsdbDocValues.getLabelsSortedSetDocValues()
        );
        assertNull("Should return null for binary labels", tsdbDocValues.getLabelsBinaryDocValues());
        assertEquals("Should have SORTED_SET storage type", LabelStorageType.SORTED_SET, tsdbDocValues.getLabelStorageType());
    }

    public void testWithBinaryLabelsStorageType() throws IOException {
        MockNumericDocValues chunkRefDocValues = new MockNumericDocValues();
        MockBinaryDocValues labelsBinaryDocValues = new MockBinaryDocValues();

        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(
            chunkRefDocValues,
            new BinaryLabelsStorage(labelsBinaryDocValues)
        );

        assertEquals("Should have BINARY storage type", LabelStorageType.BINARY, tsdbDocValues.getLabelStorageType());
        assertNull("Should return null for sorted set labels", tsdbDocValues.getLabelsSortedSetDocValues());
        assertNotNull("Should have labels storage", tsdbDocValues.getLabelsStorage());
        assertEquals("Labels storage should be BINARY type", LabelStorageType.BINARY, tsdbDocValues.getLabelsStorage().getStorageType());
    }

    public void testWithSortedSetLabelsWithNullValues() throws IOException {
        LiveSeriesIndexTSDBDocValues tsdbDocValues = LiveSeriesIndexTSDBDocValues.create(null, new SortedSetLabelsStorage(null));

        assertNull("Should return null chunk ref doc values", tsdbDocValues.getChunkRefDocValues());
        assertNull("Should return null labels sorted set doc values", tsdbDocValues.getLabelsSortedSetDocValues());
        assertEquals("Should have SORTED_SET storage type", LabelStorageType.SORTED_SET, tsdbDocValues.getLabelStorageType());

        expectThrows(UnsupportedOperationException.class, tsdbDocValues::getChunkDocValues);
    }

    static class MockNumericDocValues extends NumericDocValues {
        private long value = 0L;

        public void setValue(long value) {
            this.value = value;
        }

        @Override
        public long longValue() throws IOException {
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

    static class MockBinaryDocValues extends BinaryDocValues {
        private BytesRef value = new BytesRef("test");

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
