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
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Locale;

import static org.opensearch.common.Booleans.parseBooleanStrict;

/**
 * IntegralPlanNode represents a plan node that handles integral operations in M3QL.
 *
 * The integral function computes the cumulative sum of a series over time.
 * It takes an optional resetOnNull boolean argument (defaults to false).
 */
public class IntegralPlanNode extends M3PlanNode {
    private final boolean resetOnNull;

    /**
     * Constructor for IntegralPlanNode.
     *
     * @param id          node id
     * @param resetOnNull if true, the cumulative sum resets to zero when a null value is encountered
     */
    public IntegralPlanNode(int id, boolean resetOnNull) {
        super(id);
        this.resetOnNull = resetOnNull;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "INTEGRAL(resetOnNull=%s)", resetOnNull);
    }

    /**
     * Returns the resetOnNull flag.
     *
     * @return The resetOnNull flag
     */
    public boolean isResetOnNull() {
        return resetOnNull;
    }

    /**
     * Create an IntegralPlanNode from a FunctionNode.
     * Expected format:
     * - integral() -> defaults to resetOnNull=false
     * - integral(true) -> resetOnNull=true
     * - integral(false) -> resetOnNull=false
     *
     * @param functionNode the function node from the AST
     * @return a new IntegralPlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static IntegralPlanNode of(FunctionNode functionNode) {
        boolean resetOnNull = false; // Default

        List<M3ASTNode> childNodes = functionNode.getChildren();
        if (childNodes.size() > 1) {
            throw new IllegalArgumentException("integral function accepts at most 1 argument: resetOnNull");
        }

        if (!childNodes.isEmpty()) {
            M3ASTNode firstChild = childNodes.get(0);
            if (!(firstChild instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("integral resetOnNull argument must be a boolean value");
            }

            try {
                resetOnNull = parseBooleanStrict(valueNode.getValue(), false);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "integral resetOnNull must be a valid boolean (true/false), got: " + valueNode.getValue(),
                    e
                );
            }
        }

        return new IntegralPlanNode(M3PlannerContext.generateId(), resetOnNull);
    }
}
