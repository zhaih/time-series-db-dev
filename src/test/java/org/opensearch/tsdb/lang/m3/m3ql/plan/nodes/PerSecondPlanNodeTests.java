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
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for PerSecondPlanNode.
 */
public class PerSecondPlanNodeTests extends BasePlanNodeTests {

    public void testPerSecondPlanNodeCreation() {
        PerSecondPlanNode node = new PerSecondPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("PER_SECOND", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testPerSecondPlanNodeVisitorAccept() {
        PerSecondPlanNode node = new PerSecondPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit PerSecondPlanNode", result);
    }

    public void testPerSecondPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecond");

        PerSecondPlanNode node = PerSecondPlanNode.of(functionNode);

        assertEquals("PER_SECOND", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testPerSecondPlanNodeFactoryMethodThrowsOnArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecond");
        functionNode.addChildNode(new ValueNode("arg"));

        expectThrows(IllegalArgumentException.class, () -> PerSecondPlanNode.of(functionNode));
    }

    public void testPerSecondPlanNodeChildrenManagement() {
        PerSecondPlanNode node = new PerSecondPlanNode(1);
        M3PlanNode child = new PerSecondPlanNode(2);

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().getFirst());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(PerSecondPlanNode planNode) {
            return "visit PerSecondPlanNode";
        }
    }
}
