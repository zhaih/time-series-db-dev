/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Multiple pipelines merge, e.g. `fetch ... | fetch ... | fetch ...`
 */
public class UnionPlanNode extends M3PlanNode {

    /**
     * Constructor for UnionPlanNode.
     *
     * @param id unique identifier for the node
     */
    public UnionPlanNode(int id) {
        super(id);
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "UNION";
    }

    /**
     * Creates a UnionPlanNode from multiple M3PlanNodes.
     *
     * @param planNodes the M3PlanNodes to be unioned
     * @return a new UnionPlanNode containing the provided M3PlanNodes
     */
    public static UnionPlanNode of(M3PlanNode... planNodes) {
        UnionPlanNode unionPlanNode = new UnionPlanNode(M3PlannerContext.generateId());
        for (M3PlanNode planNode : planNodes) {
            unionPlanNode.addChild(planNode);
        }
        return unionPlanNode;
    }
}
