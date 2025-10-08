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
 * Unit tests for UnionPlanNode.
 */
public class UnionPlanNodeTests extends BasePlanNodeTests {

    public void testUnionPlanNodeCreation() {
        UnionPlanNode node = new UnionPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("UNION", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testUnionPlanNodeVisitorAccept() {
        UnionPlanNode node = new UnionPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit UnionPlanNode", result);
    }

    public void testUnionPlanNodeFactoryMethodWithNoNodes() {
        UnionPlanNode node = UnionPlanNode.of();

        assertEquals("UNION", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
        assertTrue(node.getId() >= 0);
    }

    public void testUnionPlanNodeFactoryMethodWithSingleNode() {
        M3PlanNode child = new AbsPlanNode(1);
        UnionPlanNode node = UnionPlanNode.of(child);

        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().getFirst());
    }

    public void testUnionPlanNodeFactoryMethodWithMultipleNodes() {
        M3PlanNode child1 = new AbsPlanNode(1);
        M3PlanNode child2 = new PerSecondPlanNode(2);
        M3PlanNode child3 = new RemoveEmptyPlanNode(3);

        UnionPlanNode node = UnionPlanNode.of(child1, child2, child3);

        assertEquals(3, node.getChildren().size());
        assertEquals(child1, node.getChildren().get(0));
        assertEquals(child2, node.getChildren().get(1));
        assertEquals(child3, node.getChildren().get(2));
    }

    public void testUnionPlanNodeFactoryMethodWithDifferentNodeTypes() {
        M3PlanNode fetchNode = new FetchPlanNode(1, null, null);
        M3PlanNode scaleNode = new ScalePlanNode(2, 2.0);

        UnionPlanNode node = UnionPlanNode.of(fetchNode, scaleNode);

        assertEquals(2, node.getChildren().size());
        assertEquals(fetchNode, node.getChildren().get(0));
        assertEquals(scaleNode, node.getChildren().get(1));
    }

    public void testUnionPlanNodeChildrenManagement() {
        UnionPlanNode node = new UnionPlanNode(1);
        M3PlanNode child1 = new AbsPlanNode(2);
        M3PlanNode child2 = new PerSecondPlanNode(3);

        node.addChild(child1);
        node.addChild(child2);

        assertEquals(2, node.getChildren().size());
        assertEquals(child1, node.getChildren().get(0));
        assertEquals(child2, node.getChildren().get(1));
    }

    public void testUnionPlanNodeFactoryMethodOrderPreservation() {
        M3PlanNode first = new AbsPlanNode(1);
        M3PlanNode second = new PerSecondPlanNode(2);
        M3PlanNode third = new RemoveEmptyPlanNode(3);

        UnionPlanNode node = UnionPlanNode.of(first, second, third);

        assertEquals(first, node.getChildren().get(0));
        assertEquals(second, node.getChildren().get(1));
        assertEquals(third, node.getChildren().get(2));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(UnionPlanNode planNode) {
            return "visit UnionPlanNode";
        }
    }
}
