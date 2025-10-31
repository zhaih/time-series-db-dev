/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.integrationTests;

import org.opensearch.plugins.Plugin;
import org.opensearch.tsdb.TSDBPlugin;
import org.opensearch.tsdb.framework.TimeSeriesTestFramework;

import java.util.Collection;
import java.util.List;

/**
 * Example internal cluster integration test for TSDB time series data.
 * Demonstrates the Time Series Testing Framework with internal cluster testing.
 *
 * <p>This test validates:
 * <ul>
 *   <li>Index creation with TSDB engine in internal cluster</li>
 *   <li>Time series data ingestion using internal client</li>
 *   <li>M3QL query execution using internal search client</li>
 *   <li>Response validation against expected results</li>
 * </ul>
 *
 * <p>The test configuration (data, queries, expectations) is defined in the
 * YAML file: test_cases/example_tsdb_cluster_it.yaml
 */
public class ExampleTSDBClusterIT extends TimeSeriesTestFramework {

    private static final String TEST_YAML_PATH = "test_cases/example_tsdb_cluster_it.yaml";

    @Override
    public void setUp() throws Exception {
        // Load test configuration before calling super.setUp()
        loadTestConfigurationFromFile(TEST_YAML_PATH);
        super.setUp();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TSDBPlugin.class);
    }

    /**
     * Test basic M3QL query execution via internal cluster.
     * The YAML configuration defines:
     * <ul>
     *   <li>Input time series data with HTTP request metrics</li>
     *   <li>M3QL queries for aggregation by method and status</li>
     *   <li>Expected results for validation</li>
     * </ul>
     *
     * @throws Exception If the test fails
     */
    public void testSimpleTSDBQuery() throws Exception {
        runBasicTest();
    }
}
