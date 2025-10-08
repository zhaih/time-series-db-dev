/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Plan node for rounding metric values to a specified precision.
 */
public class RoundPlanNode extends M3PlanNode {
    private final static double DEFAULT_PRECISION = 0;
    private final double precision;

    /**
     * Constructor for RoundPlanNode.
     *
     * @param id        node id
     * @param precision the precision to which values should be rounded
     */
    public RoundPlanNode(int id, double precision) {
        super(id);
        this.precision = precision;
    }

    /**
     * Returns the precision to which values should be rounded.
     *
     * @return the rounding precision
     */
    public double getPrecision() {
        return precision;
    }

    @Override
    public String getExplainName() {
        return "ROUND" + "(" + precision + ")";
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * Factory method to create a RoundPlanNode from a FunctionNode.
     * Expects the function node to represent a ROUND function with an optional precision argument.
     *
     * @param functionNode the function node representing the ROUND function
     * @return a new RoundPlanNode instance
     * @throws IllegalArgumentException if the function node has more than one argument or if the argument is not a valid number
     */
    public static RoundPlanNode of(FunctionNode functionNode) {
        double precision = DEFAULT_PRECISION;
        if (functionNode.getChildren().size() > 1) {
            throw new IllegalArgumentException("ROUND function takes at most one argument");
        }
        if (functionNode.getChildren().size() == 1) {
            if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("ROUND function argument must be a value");
            }

            try {
                precision = Double.parseDouble(valueNode.getValue());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("ROUND function argument must be an integer", e);
            }
        }
        return new RoundPlanNode(M3PlannerContext.generateId(), precision);
    }
}
