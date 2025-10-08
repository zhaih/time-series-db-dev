/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for AggregationPlanNode.
 */
public class AggregationPlanNodeTests extends BasePlanNodeTests {

    public void testAggregationPlanNodeCreation() {
        List<String> tags = Arrays.asList("tag1", "tag2");
        AggregationPlanNode node = new AggregationPlanNode(1, AggregationType.SUM, tags);

        assertEquals(1, node.getId());
        assertEquals(AggregationType.SUM, node.getAggregationType());
        assertEquals(tags, node.getTags());
        assertEquals("AGG(SUM, groupBy=[tag1, tag2])", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testAggregationPlanNodeWithNullTags() {
        AggregationPlanNode node = new AggregationPlanNode(1, AggregationType.AVG, null);

        assertEquals(AggregationType.AVG, node.getAggregationType());
        assertNull(node.getTags());
        assertEquals("AGG(AVG, groupBy=null)", node.getExplainName());
    }

    public void testAggregationPlanNodeVisitorAccept() {
        AggregationPlanNode node = new AggregationPlanNode(1, AggregationType.MAX, List.of("host"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit AggregationPlanNode", result);
    }

    public void testAggregationPlanNodeFactoryMethodWithTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sum");
        functionNode.addChildNode(new ValueNode("\"tag1\""));
        functionNode.addChildNode(new ValueNode("\"tag2\""));

        AggregationPlanNode node = AggregationPlanNode.of(functionNode, AggregationType.SUM);

        assertEquals(AggregationType.SUM, node.getAggregationType());
        assertEquals(Arrays.asList("tag1", "tag2"), node.getTags());
    }

    public void testAggregationPlanNodeFactoryMethodWithNoChildren() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("avg");

        AggregationPlanNode node = AggregationPlanNode.of(functionNode, AggregationType.AVG);

        assertEquals(AggregationType.AVG, node.getAggregationType());
        assertTrue(node.getTags().isEmpty());
    }

    public void testAggregationPlanNodeFactoryMethodWithQuotedTags() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("max");

        functionNode.addChildNode(new ValueNode("\"host\""));
        functionNode.addChildNode(new ValueNode("\"service\""));

        AggregationPlanNode node = AggregationPlanNode.of(functionNode, AggregationType.MAX);

        assertEquals(Arrays.asList("host", "service"), node.getTags());
    }

    public void testAllAggregationTypes() {
        for (AggregationType type : AggregationType.values()) {
            AggregationPlanNode node = new AggregationPlanNode(1, type, List.of("test"));
            assertEquals(type, node.getAggregationType());
            assertTrue(node.getExplainName().contains(type.toString()));
        }
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(AggregationPlanNode planNode) {
            return "visit AggregationPlanNode";
        }
    }
}
