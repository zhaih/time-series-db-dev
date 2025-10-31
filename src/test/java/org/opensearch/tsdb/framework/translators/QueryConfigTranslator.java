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

/**
 * Interface for translating different query types to OpenSearch SearchRequest.
 * This translator can be used in both REST tests and internal cluster tests.
 */
public interface QueryConfigTranslator {

    /**
     * Translate a query config to OpenSearch SearchRequest
     *
     * @param queryConfig The query config to translate
     * @param indexName The target index name
     * @return SearchRequest for OpenSearch
     * @throws Exception if translation fails
     */
    SearchRequest translate(QueryConfig queryConfig, String indexName) throws Exception;
}
