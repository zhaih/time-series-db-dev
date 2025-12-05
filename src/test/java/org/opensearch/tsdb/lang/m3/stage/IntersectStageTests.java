/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for IntersectStage.
 */
public class IntersectStageTests extends AbstractWireSerializingTestCase<IntersectStage> {

    public void testIntersectWithFullLabelMatching() {
        // Test that all labels must match exactly when no label keys specified (null or empty)

        // Left series 1: service=api, instance=server1 (exact match with right)
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=api, instance=server2 (different instance value, should NOT match)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "api", "instance", "server2");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Left series 3: service=api, instance=server1, region=us-east (extra label, should NOT match)
        List<Sample> leftSamples3 = Arrays.asList(new FloatSample(1000L, 30.0));
        ByteLabels leftLabels3 = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-east");
        TimeSeries leftSeries3 = new TimeSeries(leftSamples3, leftLabels3, 1000L, 1000L, 1000L, "left-series-3");

        // Left series 4: service=db, instance=server3 (completely different labels, should NOT match)
        List<Sample> leftSamples4 = Arrays.asList(new FloatSample(1000L, 40.0));
        ByteLabels leftLabels4 = ByteLabels.fromStrings("service", "db", "instance", "server3");
        TimeSeries leftSeries4 = new TimeSeries(leftSamples4, leftLabels4, 1000L, 1000L, 1000L, "left-series-4");

        // Right series: service=api, instance=server1
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2, leftSeries3, leftSeries4);
        List<TimeSeries> right = Arrays.asList(rightSeries);

        // Test with null label keys (full matching via leftLabels.equals(rightLabels))
        IntersectStage stageWithNull = new IntersectStage("right_series");
        List<TimeSeries> resultWithNull = stageWithNull.process(left, right);
        assertEquals(1, resultWithNull.size());
        assertTrue(resultWithNull.contains(leftSeries1)); // Only exact match
        assertFalse(resultWithNull.contains(leftSeries2)); // Different instance value
        assertFalse(resultWithNull.contains(leftSeries3)); // Extra label
        assertFalse(resultWithNull.contains(leftSeries4)); // Different labels

        // Test with empty label keys list (should behave identically to null)
        IntersectStage stageWithEmpty = new IntersectStage("right_series", new ArrayList<>());
        List<TimeSeries> resultWithEmpty = stageWithEmpty.process(left, right);
        assertEquals(1, resultWithEmpty.size());
        assertTrue(resultWithEmpty.contains(leftSeries1)); // Only exact match
    }

    public void testIntersectWithSingleLabelKey() {
        // Test selective label matching with a single label keys
        List<String> labelKeys = Arrays.asList("service"); // Only match on "service" label
        IntersectStage stage = new IntersectStage("right_series", labelKeys);

        // Left series 1: service=api, instance=server1
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=db, instance=server2 (should not match)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "db", "instance", "server2");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Left series 3: service=api, instance=server3 (should match on service)
        List<Sample> leftSamples3 = Arrays.asList(new FloatSample(1000L, 30.0));
        ByteLabels leftLabels3 = ByteLabels.fromStrings("service", "api", "instance", "server3");
        TimeSeries leftSeries3 = new TimeSeries(leftSamples3, leftLabels3, 1000L, 1000L, 1000L, "left-series-3");

        // Right series: service=api, instance=different (should match leftSeries1 and leftSeries3 on service)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "instance", "different");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2, leftSeries3);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should return leftSeries1 and leftSeries3 which have service=api
        assertEquals(2, result.size());
        assertTrue(result.contains(leftSeries1));
        assertTrue(result.contains(leftSeries3));
        assertFalse(result.contains(leftSeries2));
    }

    public void testIntersectWithMultipleLabelKeys() {
        // Test matching on multiple specific label keys
        List<String> labelKeys = Arrays.asList("service", "region");
        IntersectStage stage = new IntersectStage("right_series", labelKeys);

        // Left series 1: service=api, region=us-east, instance=server1
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api", "region", "us-east", "instance", "server1");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=api, region=us-west, instance=server2 (different region, should not match)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "api", "region", "us-west", "instance", "server2");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Left series 3: service=db, region=us-east, instance=server3 (different service, should not match)
        List<Sample> leftSamples3 = Arrays.asList(new FloatSample(1000L, 30.0));
        ByteLabels leftLabels3 = ByteLabels.fromStrings("service", "db", "region", "us-east", "instance", "server3");
        TimeSeries leftSeries3 = new TimeSeries(leftSamples3, leftLabels3, 1000L, 1000L, 1000L, "left-series-3");

        // Left series 4: service=api, region=us-east, instance=server4 (should match on service and region)
        List<Sample> leftSamples4 = Arrays.asList(new FloatSample(1000L, 40.0));
        ByteLabels leftLabels4 = ByteLabels.fromStrings("service", "api", "region", "us-east", "instance", "server4");
        TimeSeries leftSeries4 = new TimeSeries(leftSamples4, leftLabels4, 1000L, 1000L, 1000L, "left-series-4");

        // Right series: service=api, region=us-east, instance=different (should match leftSeries1 and leftSeries4)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "region", "us-east", "instance", "different");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2, leftSeries3, leftSeries4);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should return leftSeries1 and leftSeries4 which have service=api AND region=us-east
        assertEquals(2, result.size());
        assertTrue(result.contains(leftSeries1));
        assertTrue(result.contains(leftSeries4));
        assertFalse(result.contains(leftSeries2)); // Different region value
        assertFalse(result.contains(leftSeries3)); // Different service value
    }

    public void testIntersectWithMultipleRightSeries() {
        // Test matching with multiple right series
        IntersectStage stage = new IntersectStage("right_series");

        // Left series 1: service=api
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=db
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "db");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Left series 3: service=cache
        List<Sample> leftSamples3 = Arrays.asList(new FloatSample(1000L, 30.0));
        ByteLabels leftLabels3 = ByteLabels.fromStrings("service", "cache");
        TimeSeries leftSeries3 = new TimeSeries(leftSamples3, leftLabels3, 1000L, 1000L, 1000L, "left-series-3");

        // Right series 1: service=api (matches leftSeries1)
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels1 = ByteLabels.fromStrings("service", "api");
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=cache (matches leftSeries3)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 3.0));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "cache");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2, leftSeries3);
        List<TimeSeries> right = Arrays.asList(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Should match leftSeries1 (with rightSeries1) and leftSeries3 (with rightSeries2)
        // leftSeries2 doesn't match any right series
        assertEquals(2, result.size());
        assertTrue(result.contains(leftSeries1));
        assertTrue(result.contains(leftSeries3));
        assertFalse(result.contains(leftSeries2)); // No matching right series
    }

    public void testIntersectWithNoMatch() {
        // Test when no left series match any right series
        IntersectStage stage = new IntersectStage("right_series");

        // Left series: service=api
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series: service=db (different service value, no match)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "db");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should return empty list as there are no matching labels
        assertTrue(result.isEmpty());
    }

    public void testIntersectWithMissingLabelKey() {
        // Test that when a label key doesn't exist, it returns empty string and doesn't match
        List<String> labelKeys = Arrays.asList("service", "region");
        IntersectStage stage = new IntersectStage("right_series", labelKeys);

        // Left series: service=api, region=us-east
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api", "region", "us-east");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series: service=api (missing region label)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 20.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Right series: service=api, region=us-east
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should only return leftSeries1 which has both service and region labels
        // leftSeries2 has "service" but not "region" (returns empty string), so it shouldn't match
        assertEquals(1, result.size());
        assertTrue(result.contains(leftSeries1));
        assertFalse(result.contains(leftSeries2));
    }

    public void testIntersectWithMissingLabelKeyMatchesEmptyValue() {
        // Test that missing label key (get() returns "") matches explicit empty string value
        // This tests the edge case where Labels.get() returns "" for missing keys
        List<String> labelKeys = Arrays.asList("region");
        IntersectStage stage = new IntersectStage("right_series", labelKeys);

        // Left series 1: city=atlanta, dc=dca1, name=actions (empty "region" label key)
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 1.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("city", "atlanta", "dc", "dca1", "name", "actions", "region", "");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: city=chicago, dc=dca1, name=cities (missing "region" label key)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("city", "chicago", "dc", "dca1", "name", "cities");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Right series: dc=phx1, region="" (explicit empty string value for region)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, Double.NaN));
        ByteLabels rightLabels = ByteLabels.fromStrings("dc", "phx1", "region", "");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(2, result.size());
        assertTrue(result.contains(leftSeries1));
        assertTrue(result.contains(leftSeries2));
    }

    public void testIntersectWithRightMissingLabelKey() {
        List<String> labelKeys = Arrays.asList("service", "region");
        IntersectStage stage = new IntersectStage("right_series", labelKeys);

        // Left series: service=api (missing region label)
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series: service=api (missing region label)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should NOT match because both are missing the "region" label
        assertEquals(0, result.size());
    }

    public void testIntersectWithEmptyInputs() {
        IntersectStage stage = new IntersectStage("right_series");

        // Test empty left
        List<TimeSeries> emptyLeft = new ArrayList<>();
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 5.0));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");
        List<TimeSeries> right = Arrays.asList(rightSeries);

        List<TimeSeries> result1 = stage.process(emptyLeft, right);
        assertTrue("Should return empty list when left input is empty", result1.isEmpty());

        // Test empty right
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");
        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> emptyRight = new ArrayList<>();

        List<TimeSeries> result2 = stage.process(left, emptyRight);
        assertTrue("Should return empty list when right input is empty", result2.isEmpty());
    }

    public void testIntersectWithNullInputs() {
        IntersectStage stage = new IntersectStage("right_series");

        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, "series");

        // Test null left
        expectThrows(NullPointerException.class, () -> stage.process(null, Arrays.asList(series)));

        // Test null right
        expectThrows(NullPointerException.class, () -> stage.process(Arrays.asList(series), null));
    }

    public void testFactoryAndSerialization() throws IOException {
        // Test fromArgs
        Map<String, Object> args = Map.of("right_op_reference", "series2");
        IntersectStage stage = IntersectStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("intersect", stage.getName());

        // Test fromArgs with null/missing reference
        IntersectStage nullStage = IntersectStage.fromArgs(new HashMap<>());
        assertNull(nullStage.getRightOpReferenceName());

        // Test serialization
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                IntersectStage readStage = IntersectStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("intersect", readStage.getName());
            }
        }
    }

    public void testFactoryAndSerializationWithLabelKeys() throws IOException {
        // Test fromArgs with label keys
        Map<String, Object> args = Map.of("right_op_reference", "series2", "labels", Arrays.asList("service", "region"));
        IntersectStage stage = IntersectStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("intersect", stage.getName());

        // Test serialization with label keys
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                IntersectStage readStage = IntersectStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("intersect", readStage.getName());
            }
        }
    }

    private void verifyXContent(IntersectStage stage, String expectedJson) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertEquals(expectedJson, json);
    }

    public void testToXContent() throws IOException {
        // Test toXContent without labelKeys
        IntersectStage stageWithoutLabels = new IntersectStage("test_reference");
        verifyXContent(stageWithoutLabels, "{\"right_op_reference\":\"test_reference\"}");

        // Test toXContent with labelKeys
        List<String> labelKeys = Arrays.asList("service", "region");
        IntersectStage stageWithLabels = new IntersectStage("test_reference", labelKeys);
        verifyXContent(stageWithLabels, "{\"right_op_reference\":\"test_reference\",\"labels\":[\"service\",\"region\"]}");
    }

    public void testEquals() {
        // Test with default constructor
        IntersectStage stage1 = new IntersectStage("test_ref");
        IntersectStage stage2 = new IntersectStage("test_ref");

        assertEquals("Equal IntersectStages should be equal", stage1, stage2);

        IntersectStage stageDiffRef = new IntersectStage("different_ref");
        assertNotEquals("Different reference names should not be equal", stage1, stageDiffRef);

        IntersectStage stageNull1 = new IntersectStage(null);
        IntersectStage stageNull2 = new IntersectStage(null);
        assertEquals("Null reference names should be equal", stageNull1, stageNull2);

        assertNotEquals("Null vs non-null reference names should not be equal", stage1, stageNull1);
        assertNotEquals("Non-null vs null reference names should not be equal", stageNull1, stage1);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        List<String> labelKeys = Arrays.asList("service", "region");
        IntersectStage stageWithLabels1 = new IntersectStage("ref", labelKeys);
        IntersectStage stageWithLabels2 = new IntersectStage("ref", labelKeys);
        assertEquals("Stages with same reference and label keys should be equal", stageWithLabels1, stageWithLabels2);

        List<String> differentLabelKeys = Arrays.asList("service", "zone");
        IntersectStage stageWithDiffLabels = new IntersectStage("ref", differentLabelKeys);
        assertNotEquals("Stages with different label keys should not be equal", stageWithLabels1, stageWithDiffLabels);
    }

    @Override
    protected Writeable.Reader<IntersectStage> instanceReader() {
        return IntersectStage::readFrom;
    }

    @Override
    protected IntersectStage createTestInstance() {
        return new IntersectStage(randomAlphaOfLengthBetween(3, 10), randomBoolean() ? null : Arrays.asList("service", "region"));
    }
}
