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
 * DerivativePlanNode represents a plan node that handles derivative operations in M3QL.
 *
 * The derivative function calculates the rate of change from a series of values.
 * It takes no arguments.
 */
public class DerivativePlanNode extends M3PlanNode {

    /**
     * Constructor for DerivativePlanNode.
     *
     * @param id node id
     */
    public DerivativePlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "DERIVATIVE";
    }

    /**
     * Create a DerivativePlanNode from a FunctionNode.
     *
     * @param functionNode the function node from the AST
     * @return a new DerivativePlanNode instance
     * @throws IllegalArgumentException if the function has arguments
     */
    public static DerivativePlanNode of(FunctionNode functionNode) {
        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (!childNodes.isEmpty()) {
            throw new IllegalArgumentException("derivative function expects no arguments, but got " + childNodes.size());
        }

        return new DerivativePlanNode(M3PlannerContext.generateId());
    }
}
