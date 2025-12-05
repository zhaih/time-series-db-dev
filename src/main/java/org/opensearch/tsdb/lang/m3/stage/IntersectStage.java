/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pipeline stage that filters series based on label matching with a reference series.
 *
 * <p>Keeps only series from the left operand that have at least one series with matching
 * labels in the right operand. Series from left with no matching series in right are dropped.
 *
 * <p>If label keys are specified, only those label keys are compared for matching.
 * If no label keys are specified, all labels must match exactly (full label equality).
 */
@PipelineStageAnnotation(name = IntersectStage.NAME)
public class IntersectStage implements BinaryPipelineStage {

    /** The name of this pipeline stage. */
    public static final String NAME = "intersect";

    /** The parameter name for label keys. */
    public static final String LABELS_PARAM_KEY = "labels";

    private final String rightOperandReferenceName;
    private final List<String> labelKeys;

    /**
     * Constructs a new IntersectStage with the specified right operand reference name.
     *
     * @param rightOperandReferenceName the reference for the right operand
     */
    public IntersectStage(String rightOperandReferenceName) {
        this(rightOperandReferenceName, null);
    }

    /**
     * Constructs a new IntersectStage with the specified right operand reference name and label keys.
     *
     * @param rightOperandReferenceName the reference for the right operand
     * @param labelKeys                 the specific label keys to consider for matching, or null for full matching
     */
    public IntersectStage(String rightOperandReferenceName, List<String> labelKeys) {
        this.rightOperandReferenceName = rightOperandReferenceName;
        this.labelKeys = labelKeys;
    }

    /**
     * Process two time series inputs and return the intersection.
     *
     * <p>Keeps only series from the left operand that have at least one series with matching
     * labels in the right operand. If label keys are specified, only those label keys are compared.
     * If no label keys are specified, all labels must match exactly.
     *
     * @param left  The left operand time series (series to filter)
     * @param right The right operand time series (series to match against)
     * @return The filtered time series from left that have matching series in right
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left == null) {
            throw new NullPointerException(getName() + " stage received null left input");
        }
        if (right == null) {
            throw new NullPointerException(getName() + " stage received null right input");
        }
        if (left.isEmpty() || right.isEmpty()) {
            return new ArrayList<>();
        }

        // Filter left series that have matching labels with a series in right
        List<TimeSeries> result = new ArrayList<>();
        for (TimeSeries leftSeries : left) {
            if (hasMatchingSeriesInRight(leftSeries.getLabels(), right)) {
                result.add(leftSeries);
            }
        }

        return result;
    }

    /**
     * Check if any series in the right list has labels matching the given left labels.
     *
     * Matching behavior depends on whether label keys are specified:
     * if label keys are specified, only those keys must match;
     * if no label keys are specified, all labels must match exactly.
     *
     * @param leftLabels the labels to match against
     * @param right      the list of right series to search
     * @return true if at least one matching series is found
     */
    private boolean hasMatchingSeriesInRight(Labels leftLabels, List<TimeSeries> right) {
        for (TimeSeries rightSeries : right) {
            if (labelsMatch(leftLabels, rightSeries.getLabels())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if two Labels objects match based on the configured label keys.
     *
     * If labelKeys is null or empty, performs full label matching using {@link Labels#equals(Object)}.
     * Otherwise, only the specified label keys are compared for equality.
     *
     * @param leftLabels  The left labels
     * @param rightLabels The right labels
     * @return true if labels match according to the configured matching rules, false otherwise
     */
    private boolean labelsMatch(Labels leftLabels, Labels rightLabels) {
        if (leftLabels == null || rightLabels == null) {
            return false;
        }

        // If no specific label keys provided, use full label matching
        if (labelKeys == null || labelKeys.isEmpty()) {
            return leftLabels.equals(rightLabels);
        }

        // Check that all specified label keys match
        // Right operand must have the label key
        for (String labelKey : labelKeys) {
            if (!rightLabels.has(labelKey)) {
                return false;
            }

            // get() returns empty string for missing keys
            String leftValue = leftLabels.get(labelKey);
            String rightValue = rightLabels.get(labelKey);

            if (!leftValue.equals(rightValue)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
        if (labelKeys != null && !labelKeys.isEmpty()) {
            builder.field(LABELS_PARAM_KEY, labelKeys);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
        out.writeOptionalStringCollection(labelKeys);
    }

    /**
     * Create an IntersectStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new IntersectStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static IntersectStage readFrom(StreamInput in) throws IOException {
        String referenceName = in.readString();
        List<String> labelKeys = in.readOptionalStringList();
        return new IntersectStage(referenceName, labelKeys);
    }

    /**
     * Creates a new instance of IntersectStage using the provided arguments.
     *
     * @param args a map containing the stage arguments. Must contain {@code "right_op_reference"}
     *             with the reference name for the right operand. Optionally contains {@code "labels"}
     *             with a list of label keys to use for matching (if absent, all labels must match).
     * @return a new IntersectStage instance initialized with the provided arguments
     */
    @SuppressWarnings("unchecked")
    public static IntersectStage fromArgs(Map<String, Object> args) {
        String rightOpReference = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        List<String> labelKeys = (List<String>) args.get(LABELS_PARAM_KEY);
        return new IntersectStage(rightOpReference, labelKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rightOperandReferenceName, labelKeys);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        IntersectStage stage = (IntersectStage) obj;
        return Objects.equals(rightOperandReferenceName, stage.rightOperandReferenceName) && Objects.equals(labelKeys, stage.labelKeys);
    }
}
