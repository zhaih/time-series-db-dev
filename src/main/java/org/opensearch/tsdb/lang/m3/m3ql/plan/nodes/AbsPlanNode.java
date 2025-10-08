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
 * Absolute value operation node in the M3QL plan.
 */
public class AbsPlanNode extends M3PlanNode {

    /**
     * Constructor for AbsPlanNode.
     *
     * @param id unique identifier for the node
     */
    public AbsPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "ABS";
    }

    /**
     * Creates an AbsPlanNode.
     *
     * @param functionNode the function node representing the ABS function
     * @return a new AbsPlanNode
     */
    public static AbsPlanNode of(FunctionNode functionNode) {
        if (!functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("abs function must have no arguments");
        }

        return new AbsPlanNode(M3PlannerContext.generateId());
    }
}
