/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * PerSecondPlanNode represents a plan node that handles perSecond operations in M3QL.
 * The perSecond function calculates the rate of change (derivative) between consecutive
 * time points in a series, adjusted by their time intervals.
 */
public class PerSecondPlanNode extends M3PlanNode {

    /**
     * Constructor for PerSecondPlanNode.
     * @param id The unique identifier for this plan node
     */
    public PerSecondPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "PER_SECOND";
    }

    /**
     * Factory method to create PerSecondPlanNode from FunctionNode.
     * @param functionNode The function node representing perSecond
     * @return PerSecondPlanNode instance
     */
    public static PerSecondPlanNode of(FunctionNode functionNode) {
        if (!functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("perSecond function must have no arguments");
        }

        return new PerSecondPlanNode(M3PlannerContext.generateId());
    }
}
