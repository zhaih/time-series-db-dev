/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.tsdb.framework.models.QueryConfig;

/**
 * Interface for translating different query types to internal query representation
 */
public interface QueryConfigTranslator {

    /**
     * Translate a query config to internal representation
     *
     * @param queryConfig The query config to translate
     * @param indexName The target index name
     * @return Translated query result
     * @throws Exception if translation fails
     */
    Object translate(QueryConfig queryConfig, String indexName) throws Exception;
}
