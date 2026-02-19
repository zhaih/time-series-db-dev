/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationInitializationException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation Factory for filter agg
 * <br>
 * Largely forked from {@link org.opensearch.search.aggregations.bucket.filter.FilterAggregatorFactory}, except it creates
 * {@link TSDBFilterAggregator} instead
 */
public class TSDBFilterAggregatorFactory extends AggregatorFactory {

    private Weight weight;
    private final Query filter;

    public TSDBFilterAggregatorFactory(
        String name,
        QueryBuilder filterBuilder,
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, queryShardContext, parent, subFactoriesBuilder, metadata);
        filter = filterBuilder.toQuery(queryShardContext);
    }

    /**
     * Returns the {@link Weight} for this filter aggregation, creating it if
     * necessary.
     */
    public Weight getWeight() {
        if (weight == null) {
            IndexSearcher contextSearcher = queryShardContext.searcher();
            try {
                weight = contextSearcher.createWeight(contextSearcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
            } catch (IOException e) {
                throw new AggregationInitializationException("Failed to initialise filter", e);
            }
        }
        return weight;
    }

    @Override
    public Aggregator createInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        getWeight(); // eagerly initialize weight here since the getWeight might be called in concurrent context later
        return new TSDBFilterAggregator(name, this::getWeight, factories, searchContext, parent, cardinality, metadata);
    }

    @Override
    protected boolean supportsConcurrentSegmentSearch() {
        return true;
    }
}
