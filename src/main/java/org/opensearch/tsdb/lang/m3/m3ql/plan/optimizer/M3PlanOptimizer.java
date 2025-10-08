/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.optimizer;

import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;

/**
 * M3PlanOptimizer is responsible for optimizing M3PlanNodes.
 */
public class M3PlanOptimizer {

    /**
     * Private constructor to prevent instantiation.
     */
    private M3PlanOptimizer() {
        // Prevent instantiation
    }

    /**
     * Recursively optimize the given M3QL plan node.
     *
     * @param planNode The M3QL plan node to optimize
     * @return The optimized M3QL plan node
     */
    public static M3PlanNode optimize(M3PlanNode planNode) {
        // TODO: apply registered optimizer rules here
        return planNode;
    }
}
