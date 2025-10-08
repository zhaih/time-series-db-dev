/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for all M3QL plan nodes.
 */
public abstract class M3PlanNode {
    private final int id;

    private List<M3PlanNode> children;

    /**
     * Default constructor for M3PlanNode.
     * Initializes the parent to null, indicating this node has no parent.
     * @param id The ID of this plan node
     */
    public M3PlanNode(int id) {
        this.id = id;
        this.children = new ArrayList<>();
    }

    /**
     * Accept a visitor to process this plan node.
     * This is the core of the visitor pattern.
     * @param visitor the visitor to accept
     * @param <T> the return type of the visitor
     * @return the result of processing this node
     */
    public abstract <T> T accept(M3PlanVisitor<T> visitor);

    /**
     * Get a human-readable name for this node.
     * @return the explain name of this node
     */
    public abstract String getExplainName();

    /**
     * Get the children of this plan node.
     * @return the list of child plan nodes
     */
    public List<M3PlanNode> getChildren() {
        return children;
    }

    /**
     * Add a child to this plan node.
     * @param planNode the child plan node to add
     */
    public void addChild(M3PlanNode planNode) {
        children.add(planNode);
    }

    /**
     * Get the ID of this plan node.
     * @return the ID of this plan node
     */
    public int getId() {
        return id;
    }
}
