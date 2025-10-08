/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;

/**
 * RemoveEmptyPlanNode represents a node in the M3QL plan that removes series with empty samples lists.
 * This function takes no arguments and filters out series that have no data points.
 */
public class RemoveEmptyPlanNode extends M3PlanNode {

    /**
     * Constructor for RemoveEmptyPlanNode.
     *
     * @param id node id
     */
    public RemoveEmptyPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "REMOVE_EMPTY";
    }

    /**
     * Create a RemoveEmptyPlanNode from a FunctionNode.
     *
     * @param functionNode the function node from the AST
     * @return a new RemoveEmptyPlanNode instance
     * @throws IllegalArgumentException if the function has arguments
     */
    public static RemoveEmptyPlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (!childNodes.isEmpty()) {
            throw new IllegalArgumentException("RemoveEmpty function expects no arguments, but got " + childNodes.size());
        }

        return new RemoveEmptyPlanNode(M3PlannerContext.generateId());
    }
}
