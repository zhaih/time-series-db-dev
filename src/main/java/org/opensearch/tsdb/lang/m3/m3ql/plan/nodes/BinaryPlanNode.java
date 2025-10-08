/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Represents a binary operation plan node.
 */
public class BinaryPlanNode extends M3PlanNode {

    private final Type type;

    /**
     * Constructor for BinaryPlanNode.
     *
     * @param id node id
     * @param type the type of binary operation
     */
    public BinaryPlanNode(int id, Type type) {
        super(id);
        this.type = type;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return type.name();
    }

    /**
     * Returns the type of binary operation.
     *
     * @return the binary operation type
     */
    public Type getType() {
        return type;
    }

    /**
     * Enumeration representing the types of binary operations that can be performed in a pipeline.
     */
    public enum Type {
        /**
         * Add the values of two series.
         */
        AS_PERCENT,

        /**
         * Add the values of two series.
         */
        DIFF,

        /**
         * Divide the values of two series.
         */
        DIVIDE_SERIES;
    }
}
