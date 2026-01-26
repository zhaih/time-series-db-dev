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
 * Unit tests for IntegralPlanNode.
 */
public class IntegralPlanNodeTests extends BasePlanNodeTests {

    public void testIntegralPlanNodeCreation() {
        IntegralPlanNode node = new IntegralPlanNode(1, true);

        assertEquals(1, node.getId());
        assertTrue(node.isResetOnNull());
        assertTrue(node.getExplainName().contains("resetOnNull=true"));
        assertTrue(node.getChildren().isEmpty());
    }

    public void testIntegralPlanNodeVisitorAccept() {
        IntegralPlanNode node = new IntegralPlanNode(1, false);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit IntegralPlanNode", result);
    }

    public void testIntegralPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("integral");

        IntegralPlanNode node = IntegralPlanNode.of(functionNode);

        assertFalse(node.isResetOnNull());
        assertTrue(node.getId() >= 0);
    }

    public void testIntegralPlanNodeFactoryMethodWithTrue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("integral");
        functionNode.addChildNode(new ValueNode("true"));

        IntegralPlanNode node = IntegralPlanNode.of(functionNode);

        assertTrue(node.isResetOnNull());
    }

    public void testIntegralPlanNodeFactoryMethodWithFalse() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("integral");
        functionNode.addChildNode(new ValueNode("false"));

        IntegralPlanNode node = IntegralPlanNode.of(functionNode);

        assertFalse(node.isResetOnNull());
    }

    public void testIntegralPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("integral");
        functionNode.addChildNode(new ValueNode("true"));
        functionNode.addChildNode(new ValueNode("false"));

        expectThrows(IllegalArgumentException.class, () -> IntegralPlanNode.of(functionNode));
    }

    public void testIntegralPlanNodeFactoryMethodThrowsOnInvalidBoolean() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("integral");
        functionNode.addChildNode(new ValueNode("invalid"));

        expectThrows(IllegalArgumentException.class, () -> IntegralPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(IntegralPlanNode planNode) {
            return "visit IntegralPlanNode";
        }
    }
}
