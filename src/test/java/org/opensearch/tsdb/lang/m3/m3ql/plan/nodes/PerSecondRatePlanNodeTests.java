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

import java.time.Duration;

/**
 * Unit tests for PerSecondRatePlanNode.
 */
public class PerSecondRatePlanNodeTests extends BasePlanNodeTests {

    public void testPerSecondRatePlanNodeCreation() {
        PerSecondRatePlanNode node = new PerSecondRatePlanNode(1, "10s");

        assertEquals(1, node.getId());
        assertEquals(Duration.ofSeconds(10), node.getInterval());
        assertEquals("10s", node.getIntervalString());
        assertEquals("PER_SECOND_RATE(10s)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testPerSecondRatePlanNodeWithDifferentTimeUnits() {
        PerSecondRatePlanNode secondNode = new PerSecondRatePlanNode(1, "30s");
        assertEquals(Duration.ofSeconds(30), secondNode.getInterval());

        PerSecondRatePlanNode minuteNode = new PerSecondRatePlanNode(2, "5m");
        assertEquals(Duration.ofMinutes(5), minuteNode.getInterval());

        PerSecondRatePlanNode hourNode = new PerSecondRatePlanNode(3, "1h");
        assertEquals(Duration.ofHours(1), hourNode.getInterval());
    }

    public void testPerSecondRatePlanNodeVisitorAccept() {
        PerSecondRatePlanNode node = new PerSecondRatePlanNode(1, "10s");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit PerSecondRatePlanNode", result);
    }

    public void testPerSecondRatePlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecondRate");
        functionNode.addChildNode(new ValueNode("10s"));

        PerSecondRatePlanNode node = PerSecondRatePlanNode.of(functionNode);

        assertEquals(Duration.ofSeconds(10), node.getInterval());
        assertEquals("10s", node.getIntervalString());
    }

    public void testPerSecondRatePlanNodeFactoryMethodWithVariousIntervals() {
        FunctionNode functionNode1 = new FunctionNode();
        functionNode1.setFunctionName("perSecondRate");
        functionNode1.addChildNode(new ValueNode("1m"));

        PerSecondRatePlanNode node1 = PerSecondRatePlanNode.of(functionNode1);
        assertEquals(Duration.ofMinutes(1), node1.getInterval());

        FunctionNode functionNode2 = new FunctionNode();
        functionNode2.setFunctionName("perSecondRate");
        functionNode2.addChildNode(new ValueNode("2h"));

        PerSecondRatePlanNode node2 = PerSecondRatePlanNode.of(functionNode2);
        assertEquals(Duration.ofHours(2), node2.getInterval());
    }

    public void testPerSecondRatePlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecondRate");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> PerSecondRatePlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("must have exactly one argument"));
    }

    public void testPerSecondRatePlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecondRate");
        functionNode.addChildNode(new ValueNode("10s"));
        functionNode.addChildNode(new ValueNode("extra"));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> PerSecondRatePlanNode.of(functionNode));
        assertTrue(exception.getMessage().contains("must have exactly one argument"));
    }

    public void testPerSecondRatePlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("perSecondRate");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> PerSecondRatePlanNode.of(functionNode));
        assertEquals("Argument must be a value representing the interval", exception.getMessage());
    }

    public void testPerSecondRatePlanNodeGetIntervalThrowsOnNegative() {
        PerSecondRatePlanNode node = new PerSecondRatePlanNode(1, "-10s");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, node::getInterval);
        assertEquals("Interval cannot be negative: -10s", exception.getMessage());
    }

    public void testPerSecondRatePlanNodeGetIntervalThrowsOnNegativeDuration() {
        PerSecondRatePlanNode node = new PerSecondRatePlanNode(1, "-1h");

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, node::getInterval);
        assertEquals("Interval cannot be negative: -1h", exception.getMessage());
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(PerSecondRatePlanNode planNode) {
            return "visit PerSecondRatePlanNode";
        }
    }
}
