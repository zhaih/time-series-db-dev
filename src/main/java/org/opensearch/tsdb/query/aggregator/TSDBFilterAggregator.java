/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.BucketsAggregator;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.SingleBucketAggregator;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Aggregate all docs that match a filter.
 * <br>
 * Largely forked from {@link org.opensearch.search.aggregations.bucket.filter.FilterAggregator} except it initialize the
 * filter lazily
 */
public class TSDBFilterAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final Supplier<Weight> filter;

    public TSDBFilterAggregator(
        String name,
        Supplier<Weight> filter,
        AggregatorFactories factories,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);
        this.filter = filter;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {

        return new LeafBucketCollectorBase(sub, null) {
            Bits bits = null; // will be lazily initialized when we hit a doc

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bits == null) {
                    // no need to provide deleted docs to the filter
                    bits = Lucene.asSequentialAccessBits(ctx.reader().maxDoc(), filter.get().scorerSupplier(ctx));
                }
                if (bits.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalFilter(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalFilter(name, 0, buildEmptySubAggregations(), metadata());
    }

    public static class InternalFilter extends InternalSingleBucketAggregation implements Filter {
        InternalFilter(String name, long docCount, InternalAggregations subAggregations, Map<String, Object> metadata) {
            super(name, docCount, subAggregations, metadata);
        }

        /**
         * Stream from a stream.
         */
        public InternalFilter(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return TSDBFilterAggregationBuilder.NAME;
        }

        @Override
        protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
            return new InternalFilter(name, docCount, subAggregations, getMetadata());
        }
    }
}
