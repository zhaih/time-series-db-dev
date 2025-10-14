/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.translators;

import org.opensearch.tsdb.framework.models.QueryConfig;
import org.opensearch.tsdb.framework.models.TimeConfig;

/**
 * Translator for PromQL queries
 */
public class PromQLTranslator implements QueryConfigTranslator {

    @Override
    public Object translate(QueryConfig queryConfig, String indexName) throws Exception {
        String queryString = queryConfig.query();
        TimeConfig config = queryConfig.config();

        if (config == null) {
            throw new IllegalArgumentException("Query config is required for PromQL query execution");
        }

        throw new UnsupportedOperationException("PromQL translation is not yet implemented. Query: " + queryString);
    }
}
