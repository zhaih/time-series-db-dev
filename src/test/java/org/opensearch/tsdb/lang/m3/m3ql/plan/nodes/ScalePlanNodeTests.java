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
 * Unit tests for ScalePlanNode.
 */
public class ScalePlanNodeTests extends BasePlanNodeTests {

    public void testScalePlanNodeCreation() {
        ScalePlanNode node = new ScalePlanNode(1, 2.5);

        assertEquals(1, node.getId());
        assertEquals(2.5, node.getScaleFactor(), 0.0);
        assertEquals("SCALE(2.5)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testScalePlanNodeWithNegativeScale() {
        ScalePlanNode node = new ScalePlanNode(1, -1.5);

        assertEquals(-1.5, node.getScaleFactor(), 0.0);
        assertEquals("SCALE(-1.5)", node.getExplainName());
    }

    public void testScalePlanNodeWithZeroScale() {
        ScalePlanNode node = new ScalePlanNode(1, 0.0);

        assertEquals(0.0, node.getScaleFactor(), 0.0);
        assertEquals("SCALE(0.0)", node.getExplainName());
    }

    public void testScalePlanNodeVisitorAccept() {
        ScalePlanNode node = new ScalePlanNode(1, 3.14);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit ScalePlanNode", result);
    }

    public void testScalePlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new ValueNode("2.5"));

        ScalePlanNode node = ScalePlanNode.of(functionNode);

        assertEquals(2.5, node.getScaleFactor(), 0.0);
    }

    public void testScalePlanNodeFactoryMethodWithIntegerValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new ValueNode("10"));

        ScalePlanNode node = ScalePlanNode.of(functionNode);

        assertEquals(10.0, node.getScaleFactor(), 0.0);
    }

    public void testScalePlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new ValueNode("-0.5"));

        ScalePlanNode node = ScalePlanNode.of(functionNode);

        assertEquals(-0.5, node.getScaleFactor(), 0.0);
    }

    public void testScalePlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");

        expectThrows(IllegalArgumentException.class, () -> ScalePlanNode.of(functionNode));
    }

    public void testScalePlanNodeFactoryMethodThrowsOnMultipleArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new ValueNode("2.0"));
        functionNode.addChildNode(new ValueNode("3.0"));

        expectThrows(IllegalArgumentException.class, () -> ScalePlanNode.of(functionNode));
    }

    public void testScalePlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> ScalePlanNode.of(functionNode));
    }

    public void testScalePlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("scale");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(NumberFormatException.class, () -> ScalePlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(ScalePlanNode planNode) {
            return "visit ScalePlanNode";
        }
    }
}
