/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.models.TimeConfig;

/**
 * Translator for PromQL queries.
 * Converts PromQL query strings to OpenSearch SearchRequest.
 * Used in both REST tests and internal cluster tests.
 */
public class PromQLTranslator implements QueryConfigTranslator {

    @Override
    public SearchRequest translate(QueryConfig queryConfig, String indexName) throws Exception {
        String queryString = queryConfig.query();
        TimeConfig config = queryConfig.config();

        if (config == null) {
            throw new IllegalArgumentException("Query config is required for PromQL query execution");
        }

        // TODO: Implement PromQL to SearchRequest translation
        // This will involve parsing the PromQL query string and building the appropriate
        // OpenSearch aggregations and filters
        throw new UnsupportedOperationException("PromQL translation is not yet implemented. Query: " + queryString);
    }
}
