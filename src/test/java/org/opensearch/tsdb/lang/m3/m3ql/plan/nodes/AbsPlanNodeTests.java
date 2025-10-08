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
 * Unit tests for AbsPlanNode.
 */
public class AbsPlanNodeTests extends BasePlanNodeTests {

    public void testAbsPlanNodeCreation() {
        AbsPlanNode node = new AbsPlanNode(1);

        assertEquals(1, node.getId());
        assertEquals("ABS", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testAbsPlanNodeVisitorAccept() {
        AbsPlanNode node = new AbsPlanNode(1);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit AbsPlanNode", result);
    }

    public void testAbsPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("abs");
        AbsPlanNode node = AbsPlanNode.of(functionNode);

        assertNotNull(node);
        assertEquals("ABS", node.getExplainName());
        assertTrue(node.getId() >= 0);
    }

    public void testAbsPlanNodeFactoryMethodNoChildren() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("abs");
        ValueNode valueNode = new ValueNode("val");
        functionNode.addChildNode(valueNode);

        expectThrows(IllegalArgumentException.class, () -> AbsPlanNode.of(functionNode));
    }

    public void testAbsPlanNodeChildrenManagement() {
        AbsPlanNode node = new AbsPlanNode(1);
        M3PlanNode child = new AbsPlanNode(2);

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
        public String visit(AbsPlanNode planNode) {
            return "visit AbsPlanNode";
        }
    }
}
