/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for TSDBFilterAggregationBuilder.
 */
public class TSDBFilterAggregationBuilderTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        MatchAllQueryBuilder filter = new MatchAllQueryBuilder();
        TSDBFilterAggregationBuilder builder = new TSDBFilterAggregationBuilder("my_filter", filter);

        assertEquals("my_filter", builder.getName());
        assertSame(filter, builder.getFilter());
        assertEquals("tsdb_filter", builder.getType());
        assertEquals(AggregationBuilder.BucketCardinality.ONE, builder.bucketCardinality());
    }

    public void testConstructorWithNullFilterThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TSDBFilterAggregationBuilder("bad", null));
        assertTrue(e.getMessage().contains("[filter] must not be null"));
        assertTrue(e.getMessage().contains("bad"));
    }

    public void testShallowCopy() {
        MatchAllQueryBuilder filter = new MatchAllQueryBuilder();
        TSDBFilterAggregationBuilder original = new TSDBFilterAggregationBuilder("orig", filter);

        AggregationBuilder copy = original.shallowCopy(mock(AggregatorFactories.Builder.class), Map.of("k", "v"));

        assertTrue(copy instanceof TSDBFilterAggregationBuilder);
        TSDBFilterAggregationBuilder typedCopy = (TSDBFilterAggregationBuilder) copy;
        assertEquals("orig", typedCopy.getName());
        assertSame(filter, typedCopy.getFilter());
    }

    public void testEqualsAndHashCode() {
        MatchAllQueryBuilder filter = new MatchAllQueryBuilder();
        TSDBFilterAggregationBuilder a = new TSDBFilterAggregationBuilder("test", filter);
        TSDBFilterAggregationBuilder b = new TSDBFilterAggregationBuilder("test", filter);
        TSDBFilterAggregationBuilder diffName = new TSDBFilterAggregationBuilder("other", filter);
        TSDBFilterAggregationBuilder diffFilter = new TSDBFilterAggregationBuilder("test", new TermQueryBuilder("f", "v"));

        assertTrue(a.equals(b));
        assertEquals(a.hashCode(), b.hashCode());
        assertFalse(a.equals(diffName));
        assertFalse(a.equals(diffFilter));
        assertFalse(a.equals(null));
    }

    public void testXContentGeneration() throws IOException {
        TSDBFilterAggregationBuilder builder = new TSDBFilterAggregationBuilder("x", new MatchAllQueryBuilder());

        XContentBuilder xContent = XContentFactory.jsonBuilder();
        xContent.startObject();
        builder.toXContent(xContent, null);
        xContent.endObject();

        String json = xContent.toString();
        assertTrue(json.contains("tsdb_filter"));
        assertTrue(json.contains("match_all"));
    }

    public void testBuilderSerialization() throws IOException {
        TSDBFilterAggregationBuilder original = new TSDBFilterAggregationBuilder("test_filter", new MatchAllQueryBuilder());
        NamedWriteableRegistry registry = new NamedWriteableRegistry(
            List.of(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new))
        );

        TSDBFilterAggregationBuilder deserialized = copyWriteable(original, registry, TSDBFilterAggregationBuilder::new);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getFilter(), deserialized.getFilter());
        assertEquals(original.getType(), deserialized.getType());
    }

    public void testInternalFilterSerialization() throws IOException {
        TSDBFilterAggregator.InternalFilter original = new TSDBFilterAggregator.InternalFilter(
            "filter_result",
            42,
            InternalAggregations.EMPTY,
            Map.of()
        );
        NamedWriteableRegistry registry = new NamedWriteableRegistry(List.of());

        TSDBFilterAggregator.InternalFilter deserialized = copyWriteable(original, registry, TSDBFilterAggregator.InternalFilter::new);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getDocCount(), deserialized.getDocCount());
        assertEquals(TSDBFilterAggregationBuilder.NAME, deserialized.getWriteableName());
    }
}
