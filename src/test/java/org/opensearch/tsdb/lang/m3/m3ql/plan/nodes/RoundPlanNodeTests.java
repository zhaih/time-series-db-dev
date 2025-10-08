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
 * Unit tests for RoundPlanNode.
 */
public class RoundPlanNodeTests extends BasePlanNodeTests {

    public void testRoundPlanNodeVisitorAccept() {
        RoundPlanNode node = new RoundPlanNode(1, 2);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit RoundPlanNode", result);
    }

    public void testRoundPlanNodeFactoryMethodWithNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");

        RoundPlanNode node = RoundPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals(0.0, node.getPrecision(), 0.0);
        assertEquals("ROUND(0.0)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testRoundPlanNodeFactoryMethodWithPrecisionArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");
        ValueNode precisionNode = new ValueNode("5");
        functionNode.addChildNode(precisionNode);

        RoundPlanNode node = RoundPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals(5.0, node.getPrecision(), 0.0);
        assertEquals("ROUND(5.0)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testRoundPlanNodeFactoryMethodWithNegativePrecision() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");
        ValueNode precisionNode = new ValueNode("-3");
        functionNode.addChildNode(precisionNode);

        RoundPlanNode node = RoundPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals(-3.0, node.getPrecision(), 0.0);
        assertEquals("ROUND(-3.0)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testRoundPlanNodeFactoryMethodWithZeroPrecision() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");
        ValueNode precisionNode = new ValueNode("0");
        functionNode.addChildNode(precisionNode);

        RoundPlanNode node = RoundPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals(0.0, node.getPrecision(), 0.0);
        assertEquals("ROUND(0.0)", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testRoundPlanNodeFactoryMethodWithInvalidPrecision() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");
        ValueNode precisionNode = new ValueNode("abc");
        functionNode.addChildNode(precisionNode);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RoundPlanNode.of(functionNode));
        assertEquals("ROUND function argument must be an integer", exception.getMessage());
    }

    public void testRoundPlanNodeFactoryMethodWithTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("round");
        ValueNode precisionNode1 = new ValueNode("2");
        ValueNode precisionNode2 = new ValueNode("3");
        functionNode.addChildNode(precisionNode1);
        functionNode.addChildNode(precisionNode2);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RoundPlanNode.of(functionNode));
        assertEquals("ROUND function takes at most one argument", exception.getMessage());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(RoundPlanNode node) {
            return "visit RoundPlanNode";
        }
    }
}
