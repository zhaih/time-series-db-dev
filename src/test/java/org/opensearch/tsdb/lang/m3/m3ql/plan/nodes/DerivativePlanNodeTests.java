/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

/**
 * Unit tests for DerivativePlanNode.
 */
public class DerivativePlanNodeTests extends BasePlanNodeTests {

    public void testDerivativePlanNodeCreation() {
        DerivativePlanNode node = new DerivativePlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("DERIVATIVE", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testDerivativePlanNodeVisitorAccept() {
        DerivativePlanNode node = new DerivativePlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit DerivativePlanNode", result);
    }

    public void testDerivativePlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("derivative");

        DerivativePlanNode node = DerivativePlanNode.of(functionNode);

        assertEquals("DERIVATIVE", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testDerivativePlanNodeFactoryMethodThrowsOnArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("derivative");
        functionNode.addChildNode(new org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode("arg"));

        expectThrows(IllegalArgumentException.class, () -> DerivativePlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(DerivativePlanNode planNode) {
            return "visit DerivativePlanNode";
        }
    }
}
