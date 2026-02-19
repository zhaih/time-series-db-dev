/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Largely forked from {@link org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder} except it build
 * towards {@link TSDBFilterAggregatorFactory} and has a name of 'tsdb_filter' instead of original 'filter'
 */
public class TSDBFilterAggregationBuilder extends AbstractAggregationBuilder<TSDBFilterAggregationBuilder> {
    public static final String NAME = "tsdb_filter";

    private final QueryBuilder filter;

    /**
     * @param name
     *            the name of this aggregation
     * @param filter
     *            Set the filter to use, only documents that match this
     *            filter will fall into the bucket defined by this
     *            {@link Filter} aggregation.
     */
    public TSDBFilterAggregationBuilder(String name, QueryBuilder filter) {
        super(name);
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null: [" + name + "]");
        }
        this.filter = filter;
    }

    protected TSDBFilterAggregationBuilder(
        TSDBFilterAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.filter = clone.filter;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TSDBFilterAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public TSDBFilterAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        filter = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder result = Rewriteable.rewrite(filter, queryShardContext);
        if (result != filter) {
            return new TSDBFilterAggregationBuilder(getName(), result);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new TSDBFilterAggregatorFactory(name, filter, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            filter.toXContent(builder, params);
        }
        return builder;
    }

    public static TSDBFilterAggregationBuilder parse(XContentParser parser, String aggregationName) throws IOException {
        QueryBuilder filter = parseInnerQueryBuilder(parser);
        return new TSDBFilterAggregationBuilder(aggregationName, filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        TSDBFilterAggregationBuilder other = (TSDBFilterAggregationBuilder) obj;
        return Objects.equals(filter, other.filter);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    /**
     * Register aggregators with the values source registry.
     *
     * @param builder The values source registry builder
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        // Register usage for the aggregation since we don't use ValuesSourceRegistry
        // but still need to be registered for usage tracking
        builder.registerUsage(NAME);
    }
}
