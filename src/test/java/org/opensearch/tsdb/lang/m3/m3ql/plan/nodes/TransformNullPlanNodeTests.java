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
 * Unit tests for TransformNullPlanNode.
 */
public class TransformNullPlanNodeTests extends BasePlanNodeTests {

    public void testTransformNullPlanNodeCreation() {
        TransformNullPlanNode node = new TransformNullPlanNode(1, 5.0);

        assertEquals(1, node.getId());
        assertEquals(5.0, node.getFillValue(), 0.0);
        assertEquals("TRANSFORM_NULL(5.0)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testTransformNullPlanNodeWithZeroFillValue() {
        TransformNullPlanNode node = new TransformNullPlanNode(1, 0.0);

        assertEquals(0.0, node.getFillValue(), 0.0);
        assertEquals("TRANSFORM_NULL(0.0)", node.getExplainName());
    }

    public void testTransformNullPlanNodeWithNegativeFillValue() {
        TransformNullPlanNode node = new TransformNullPlanNode(1, -1.5);

        assertEquals(-1.5, node.getFillValue(), 0.0);
        assertEquals("TRANSFORM_NULL(-1.5)", node.getExplainName());
    }

    public void testTransformNullPlanNodeVisitorAccept() {
        TransformNullPlanNode node = new TransformNullPlanNode(1, 3.14);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit TransformNullPlanNode", result);
    }

    public void testTransformNullPlanNodeFactoryMethodWithArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new ValueNode("3.14"));

        TransformNullPlanNode node = TransformNullPlanNode.of(functionNode);

        assertEquals(3.14, node.getFillValue(), 0.0);
    }

    public void testTransformNullPlanNodeFactoryMethodWithoutArgument() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");

        TransformNullPlanNode node = TransformNullPlanNode.of(functionNode);

        assertEquals(0.0, node.getFillValue(), 0.0);
    }

    public void testTransformNullPlanNodeFactoryMethodWithIntegerValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new ValueNode("10"));

        TransformNullPlanNode node = TransformNullPlanNode.of(functionNode);

        assertEquals(10.0, node.getFillValue(), 0.0);
    }

    public void testTransformNullPlanNodeFactoryMethodWithNegativeValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new ValueNode("-2.5"));

        TransformNullPlanNode node = TransformNullPlanNode.of(functionNode);

        assertEquals(-2.5, node.getFillValue(), 0.0);
    }

    public void testTransformNullPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new ValueNode("1.0"));
        functionNode.addChildNode(new ValueNode("2.0"));

        expectThrows(IllegalArgumentException.class, () -> TransformNullPlanNode.of(functionNode));
    }

    public void testTransformNullPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> TransformNullPlanNode.of(functionNode));
    }

    public void testTransformNullPlanNodeFactoryMethodThrowsOnInvalidNumber() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("transformNull");
        functionNode.addChildNode(new ValueNode("not_a_number"));

        expectThrows(IllegalArgumentException.class, () -> TransformNullPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(TransformNullPlanNode planNode) {
            return "visit TransformNullPlanNode";
        }
    }
}
