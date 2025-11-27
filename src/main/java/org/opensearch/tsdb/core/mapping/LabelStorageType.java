/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.mapping;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.BinaryLabelsStorage;
import org.opensearch.tsdb.core.reader.LabelsStorage;
import org.opensearch.tsdb.core.reader.SortedSetLabelsStorage;

import java.io.IOException;
import java.util.Locale;

/**
 * Enum representing the storage type for label doc values in TSDB indices.
 *
 * <p>This enum serves as a factory for creating {@link LabelsStorage} instances
 * and handles write-time operations (adding labels to documents). Each storage
 * type creates the appropriate storage implementation that handles its own
 * read logic.</p>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * // Writing: Add labels to a document
 * labelStorageType.addLabelsToDocument(doc, labels, labelRefs);
 *
 * // Reading: Get storage from leaf reader, then read labels
 * LabelsStorage storage = labelStorageType.getLabelsStorage(leafReader);
 * Labels labels = storage.readLabels(docId);
 * }</pre>
 */
public enum LabelStorageType {
    /**
     * Binary storage: Labels are serialized as BinaryDocValues.
     * Faster reads/writes, lower memory overhead, recommended for most use cases.
     */
    BINARY("binary") {
        @Override
        public void addLabelsToDocument(Document doc, Labels labels, BytesRef[] cachedLabelRefs) {
            // BINARY storage doesn't use the cached refs - it uses the raw bytes directly
            if (!(labels instanceof ByteLabels byteLabels)) {
                throw new IllegalArgumentException("BINARY storage requires ByteLabels implementation");
            }
            BytesRef serializedLabels = new BytesRef(byteLabels.getRawBytes());
            doc.add(new BinaryDocValuesField(Constants.IndexSchema.LABELS, serializedLabels));
        }

        @Override
        public LabelsStorage getLabelsStorage(LeafReader leafReader) throws IOException {
            BinaryDocValues docValues = leafReader.getBinaryDocValues(Constants.IndexSchema.LABELS);
            return new BinaryLabelsStorage(docValues);
        }

        @Override
        public LabelsStorage getLabelsStorageOrThrow(LeafReader leafReader, String contextMessage) throws IOException {
            BinaryDocValues docValues = leafReader.getBinaryDocValues(Constants.IndexSchema.LABELS);
            if (docValues == null) {
                throw new IOException(
                    "Labels field '"
                        + Constants.IndexSchema.LABELS
                        + "' not found"
                        + (contextMessage != null ? " " + contextMessage : "")
                        + "."
                );
            }
            return new BinaryLabelsStorage(docValues);
        }
    },

    /**
     * Sorted set storage: Labels are stored as SortedSetDocValues.
     * Better compression for high cardinality labels, allows efficient range queries.
     */
    SORTED_SET("sorted_set") {
        @Override
        public void addLabelsToDocument(Document doc, Labels labels, BytesRef[] cachedLabelRefs) {
            for (BytesRef labelRef : cachedLabelRefs) {
                doc.add(new SortedSetDocValuesField(Constants.IndexSchema.LABELS, labelRef));
            }
        }

        @Override
        public LabelsStorage getLabelsStorage(LeafReader leafReader) throws IOException {
            SortedSetDocValues docValues = leafReader.getSortedSetDocValues(Constants.IndexSchema.LABELS);
            return new SortedSetLabelsStorage(docValues);
        }

        @Override
        public LabelsStorage getLabelsStorageOrThrow(LeafReader leafReader, String contextMessage) throws IOException {
            SortedSetDocValues docValues = leafReader.getSortedSetDocValues(Constants.IndexSchema.LABELS);
            if (docValues == null) {
                throw new IOException(
                    "Labels sorted set field '"
                        + Constants.IndexSchema.LABELS
                        + "' not found"
                        + (contextMessage != null ? " " + contextMessage : "")
                        + "."
                );
            }
            return new SortedSetLabelsStorage(docValues);
        }
    };

    private final String value;

    LabelStorageType(String value) {
        this.value = value;
    }

    /**
     * Get the string representation of the storage type.
     *
     * @return the storage type as a string
     */
    public String getValue() {
        return value;
    }

    /**
     * Parse a string value to a LabelStorageType enum.
     *
     * @param value the string value to parse
     * @return the corresponding LabelStorageType
     * @throws IllegalArgumentException if the value is not a valid storage type
     */
    public static LabelStorageType fromString(String value) {
        String normalized = value.toLowerCase(Locale.ROOT);
        for (LabelStorageType type : values()) {
            if (type.value.equals(normalized)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid label storage type: " + value + ". Valid values are: binary, sorted_set");
    }

    @Override
    public String toString() {
        return value;
    }

    // ========== Abstract methods ==========

    /**
     * Add labels to a Lucene Document for DocValues storage.
     *
     * @param doc the document to add labels to
     * @param labels the labels to add
     * @param cachedLabelRefs pre-computed label key-value BytesRefs to avoid duplicate computation
     *                        (used by SORTED_SET, ignored by BINARY)
     */
    public abstract void addLabelsToDocument(Document doc, Labels labels, BytesRef[] cachedLabelRefs);

    /**
     * Create a LabelsStorage from a LeafReader.
     *
     * @param leafReader the leaf reader to get doc values from
     * @return the LabelsStorage for reading labels (may wrap null doc values if field doesn't exist)
     * @throws IOException if an I/O error occurs
     */
    public abstract LabelsStorage getLabelsStorage(LeafReader leafReader) throws IOException;

    /**
     * Create a LabelsStorage from a LeafReader, throwing an exception if the field is not found.
     *
     * @param leafReader the leaf reader to get doc values from
     * @param contextMessage optional context message to include in the exception (e.g., "in live series index")
     * @return the LabelsStorage for reading labels
     * @throws IOException if the field is not found or an I/O error occurs
     */
    public abstract LabelsStorage getLabelsStorageOrThrow(LeafReader leafReader, String contextMessage) throws IOException;
}
