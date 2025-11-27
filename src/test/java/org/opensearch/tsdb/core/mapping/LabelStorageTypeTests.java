/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.mapping;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.BinaryLabelsStorage;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.SortedSetLabelsStorage;

import java.io.IOException;

/**
 * Tests for {@link LabelStorageType} enum including factory methods and
 * storage operations for both BINARY and SORTED_SET storage types.
 */
public class LabelStorageTypeTests extends OpenSearchTestCase {

    // ==================== Basic Enum Tests ====================

    public void testGetValue() {
        assertEquals("binary", LabelStorageType.BINARY.getValue());
        assertEquals("sorted_set", LabelStorageType.SORTED_SET.getValue());
    }

    public void testToString() {
        assertEquals("binary", LabelStorageType.BINARY.toString());
        assertEquals("sorted_set", LabelStorageType.SORTED_SET.toString());
    }

    public void testFromStringWithValidBinaryValue() {
        assertEquals(LabelStorageType.BINARY, LabelStorageType.fromString("binary"));
        assertEquals(LabelStorageType.BINARY, LabelStorageType.fromString("BINARY"));
        assertEquals(LabelStorageType.BINARY, LabelStorageType.fromString("BiNaRy"));
    }

    public void testFromStringWithValidSortedSetValue() {
        assertEquals(LabelStorageType.SORTED_SET, LabelStorageType.fromString("sorted_set"));
        assertEquals(LabelStorageType.SORTED_SET, LabelStorageType.fromString("SORTED_SET"));
        assertEquals(LabelStorageType.SORTED_SET, LabelStorageType.fromString("SoRtEd_SeT"));
    }

    public void testFromStringWithInvalidValue() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> LabelStorageType.fromString("invalid"));
        assertTrue(exception.getMessage().contains("Invalid label storage type: invalid"));
        assertTrue(exception.getMessage().contains("Valid values are: binary, sorted_set"));
    }

    public void testFromStringWithNullValue() {
        expectThrows(NullPointerException.class, () -> LabelStorageType.fromString(null));
    }

    public void testFromStringWithEmptyValue() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> LabelStorageType.fromString(""));
        assertTrue(exception.getMessage().contains("Invalid label storage type"));
    }

    public void testAllEnumValuesHaveValidStringRepresentation() {
        for (LabelStorageType type : LabelStorageType.values()) {
            assertNotNull(type.getValue());
            assertFalse(type.getValue().isEmpty());
            assertEquals(type, LabelStorageType.fromString(type.getValue()));
        }
    }

    // ==================== addLabelsToDocument Tests ====================

    public void testBinaryAddLabelsToDocument() {
        Document doc = new Document();
        Labels labels = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1");
        BytesRef[] cachedRefs = labels.toKeyValueBytesRefs();

        LabelStorageType.BINARY.addLabelsToDocument(doc, labels, cachedRefs);

        // Verify BinaryDocValuesField was added to LABELS field
        assertNotNull(doc.getBinaryValue(Constants.IndexSchema.LABELS));
    }

    public void testBinaryAddEmptyLabelsToDocument() {
        Document doc = new Document();
        Labels emptyLabels = ByteLabels.emptyLabels();
        BytesRef[] cachedRefs = emptyLabels.toKeyValueBytesRefs();

        LabelStorageType.BINARY.addLabelsToDocument(doc, emptyLabels, cachedRefs);

        assertNotNull(doc.getBinaryValue(Constants.IndexSchema.LABELS));
    }

    public void testSortedSetAddLabelsToDocument() {
        Document doc = new Document();
        Labels labels = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1");
        BytesRef[] cachedRefs = labels.toKeyValueBytesRefs();

        LabelStorageType.SORTED_SET.addLabelsToDocument(doc, labels, cachedRefs);

        // Verify SortedSetDocValuesFields were added (one per label)
        BytesRef[] values = doc.getBinaryValues(Constants.IndexSchema.LABELS);
        assertEquals(2, values.length);
    }

    public void testSortedSetAddEmptyLabelsToDocument() {
        Document doc = new Document();
        Labels emptyLabels = ByteLabels.emptyLabels();
        BytesRef[] cachedRefs = emptyLabels.toKeyValueBytesRefs();

        LabelStorageType.SORTED_SET.addLabelsToDocument(doc, emptyLabels, cachedRefs);

        // Empty labels should result in no fields added
        BytesRef[] values = doc.getBinaryValues(Constants.IndexSchema.LABELS);
        assertEquals(0, values.length);
    }

    // ==================== getLabelsStorage Tests ====================

    public void testBinaryGetLabelsStorageReturnsCorrectType() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithBinaryLabels(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                LabelsStorage storage = LabelStorageType.BINARY.getLabelsStorage(leafReader);

                assertNotNull(storage);
                assertTrue(storage instanceof BinaryLabelsStorage);
                assertEquals(LabelStorageType.BINARY, storage.getStorageType());
            }
        }
    }

    public void testBinaryGetLabelsStorageWithMissingField() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createEmptyIndex(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                LabelsStorage storage = LabelStorageType.BINARY.getLabelsStorage(leafReader);

                assertNotNull(storage);
                assertTrue(storage instanceof BinaryLabelsStorage);
                // Reading from storage with null doc values should return empty labels
                Labels result = storage.readLabels(0);
                assertTrue(result.isEmpty());
            }
        }
    }

    public void testSortedSetGetLabelsStorageReturnsCorrectType() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithSortedSetLabels(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                LabelsStorage storage = LabelStorageType.SORTED_SET.getLabelsStorage(leafReader);

                assertNotNull(storage);
                assertTrue(storage instanceof SortedSetLabelsStorage);
                assertEquals(LabelStorageType.SORTED_SET, storage.getStorageType());
            }
        }
    }

    // ==================== getLabelsStorageOrThrow Tests ====================

    public void testBinaryGetLabelsStorageOrThrowWithValidField() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithBinaryLabels(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                LabelsStorage storage = LabelStorageType.BINARY.getLabelsStorageOrThrow(leafReader, "in test");

                assertNotNull(storage);
                assertTrue(storage instanceof BinaryLabelsStorage);
            }
        }
    }

    public void testBinaryGetLabelsStorageOrThrowWithMissingFieldAndContextMessage() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createEmptyIndex(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                IOException exception = expectThrows(
                    IOException.class,
                    () -> LabelStorageType.BINARY.getLabelsStorageOrThrow(leafReader, "in test context")
                );

                assertTrue(exception.getMessage().contains(Constants.IndexSchema.LABELS));
                assertTrue(exception.getMessage().contains("not found"));
                assertTrue(exception.getMessage().contains("in test context"));
            }
        }
    }

    public void testBinaryGetLabelsStorageOrThrowWithMissingFieldAndNullContextMessage() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createEmptyIndex(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                IOException exception = expectThrows(
                    IOException.class,
                    () -> LabelStorageType.BINARY.getLabelsStorageOrThrow(leafReader, null)
                );

                assertTrue(exception.getMessage().contains(Constants.IndexSchema.LABELS));
                assertTrue(exception.getMessage().contains("not found"));
                assertFalse(exception.getMessage().contains("null"));
            }
        }
    }

    public void testSortedSetGetLabelsStorageOrThrowWithValidField() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createIndexWithSortedSetLabels(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                LabelsStorage storage = LabelStorageType.SORTED_SET.getLabelsStorageOrThrow(leafReader, "in test");

                assertNotNull(storage);
                assertTrue(storage instanceof SortedSetLabelsStorage);
            }
        }
    }

    public void testSortedSetGetLabelsStorageOrThrowWithMissingFieldAndContextMessage() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createEmptyIndex(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                IOException exception = expectThrows(
                    IOException.class,
                    () -> LabelStorageType.SORTED_SET.getLabelsStorageOrThrow(leafReader, "in closed chunk index")
                );

                assertTrue(exception.getMessage().contains(Constants.IndexSchema.LABELS));
                assertTrue(exception.getMessage().contains("not found"));
                assertTrue(exception.getMessage().contains("in closed chunk index"));
            }
        }
    }

    public void testSortedSetGetLabelsStorageOrThrowWithMissingFieldAndNullContextMessage() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            createEmptyIndex(dir);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                IOException exception = expectThrows(
                    IOException.class,
                    () -> LabelStorageType.SORTED_SET.getLabelsStorageOrThrow(leafReader, null)
                );

                assertTrue(exception.getMessage().contains(Constants.IndexSchema.LABELS));
                assertTrue(exception.getMessage().contains("not found"));
                assertFalse(exception.getMessage().contains("null"));
            }
        }
    }

    // ==================== Integration Tests ====================

    public void testBinaryRoundTripThroughIndex() throws IOException {
        Labels originalLabels = ByteLabels.fromStrings("__name__", "test_metric", "env", "prod", "region", "us-west");

        try (Directory dir = new ByteBuffersDirectory()) {
            // Write labels to index
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                BytesRef[] cachedRefs = originalLabels.toKeyValueBytesRefs();
                LabelStorageType.BINARY.addLabelsToDocument(doc, originalLabels, cachedRefs);
                writer.addDocument(doc);
                writer.commit();
            }

            // Read labels back from index
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                LabelsStorage storage = LabelStorageType.BINARY.getLabelsStorageOrThrow(leafReader, null);

                Labels readLabels = storage.readLabels(0);

                assertEquals(originalLabels, readLabels);
            }
        }
    }

    public void testSortedSetRoundTripThroughIndex() throws IOException {
        Labels originalLabels = ByteLabels.fromStrings("__name__", "test_metric", "env", "prod", "region", "us-west");

        try (Directory dir = new ByteBuffersDirectory()) {
            // Write labels to index
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                BytesRef[] cachedRefs = originalLabels.toKeyValueBytesRefs();
                LabelStorageType.SORTED_SET.addLabelsToDocument(doc, originalLabels, cachedRefs);
                writer.addDocument(doc);
                writer.commit();
            }

            // Read labels back from index
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                LabelsStorage storage = LabelStorageType.SORTED_SET.getLabelsStorageOrThrow(leafReader, null);

                Labels readLabels = storage.readLabels(0);

                assertEquals(originalLabels, readLabels);
            }
        }
    }

    // ==================== Helper Methods ====================

    private void createIndexWithBinaryLabels(Directory dir) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            Labels labels = ByteLabels.fromStrings("key", "value");
            BytesRef[] cachedRefs = labels.toKeyValueBytesRefs();
            LabelStorageType.BINARY.addLabelsToDocument(doc, labels, cachedRefs);
            writer.addDocument(doc);
            writer.commit();
        }
    }

    private void createIndexWithSortedSetLabels(Directory dir) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            Labels labels = ByteLabels.fromStrings("key", "value");
            BytesRef[] cachedRefs = labels.toKeyValueBytesRefs();
            LabelStorageType.SORTED_SET.addLabelsToDocument(doc, labels, cachedRefs);
            writer.addDocument(doc);
            writer.commit();
        }
    }

    private void createEmptyIndex(Directory dir) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new org.apache.lucene.document.StringField("dummy", "value", org.apache.lucene.document.Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
        }
    }
}
