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
 * Unit tests for RemoveEmptyPlanNode.
 */
public class RemoveEmptyPlanNodeTests extends BasePlanNodeTests {

    public void testRemoveEmptyPlanNodeCreation() {
        RemoveEmptyPlanNode node = new RemoveEmptyPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("REMOVE_EMPTY", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testRemoveEmptyPlanNodeVisitorAccept() {
        RemoveEmptyPlanNode node = new RemoveEmptyPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit RemoveEmptyPlanNode", result);
    }

    public void testRemoveEmptyPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("removeEmpty");

        RemoveEmptyPlanNode node = RemoveEmptyPlanNode.of(functionNode);

        assertEquals("REMOVE_EMPTY", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testRemoveEmptyPlanNodeFactoryMethodThrowsOnArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("removeEmpty");
        functionNode.addChildNode(new ValueNode("arg"));

        expectThrows(IllegalArgumentException.class, () -> RemoveEmptyPlanNode.of(functionNode));
    }

    public void testRemoveEmptyPlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("removeEmpty");
        functionNode.addChildNode(new ValueNode("arg1"));
        functionNode.addChildNode(new ValueNode("arg2"));

        expectThrows(IllegalArgumentException.class, () -> RemoveEmptyPlanNode.of(functionNode));
    }

    public void testRemoveEmptyPlanNodeChildrenManagement() {
        RemoveEmptyPlanNode node = new RemoveEmptyPlanNode(1);
        M3PlanNode child = new RemoveEmptyPlanNode(2);

        node.addChild(child);
        assertEquals(1, node.getChildren().size());
        assertEquals(child, node.getChildren().get(0));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(RemoveEmptyPlanNode planNode) {
            return "visit RemoveEmptyPlanNode";
        }
    }
}
