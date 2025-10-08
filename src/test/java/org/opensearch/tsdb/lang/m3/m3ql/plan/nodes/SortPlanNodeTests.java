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
 * Unit tests for SortPlanNode.
 */
public class SortPlanNodeTests extends BasePlanNodeTests {

    public void testSortPlanNodeCreation() {
        SortPlanNode node = new SortPlanNode(1, "avg", "desc");

        assertEquals(1, node.getId());
        assertEquals("avg", node.getSortBy());
        assertEquals("desc", node.getSortOrder());
        assertEquals("SORT(avg, desc)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testSortPlanNodeWithAscendingOrder() {
        SortPlanNode node = new SortPlanNode(1, "max", "asc");

        assertEquals("max", node.getSortBy());
        assertEquals("asc", node.getSortOrder());
        assertEquals("SORT(max, asc)", node.getExplainName());
    }

    public void testSortPlanNodeVisitorAccept() {
        SortPlanNode node = new SortPlanNode(1, "sum", "desc");
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit SortPlanNode", result);
    }

    public void testSortPlanNodeFactoryMethodWithBothArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("max"));
        functionNode.addChildNode(new ValueNode("asc"));

        SortPlanNode node = SortPlanNode.of(functionNode);

        assertEquals("max", node.getSortBy());
        assertEquals("asc", node.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodWithDefaultOrder() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("sum"));

        SortPlanNode node = SortPlanNode.of(functionNode);

        assertEquals("sum", node.getSortBy());
        assertEquals("desc", node.getSortOrder());
    }

    public void testSortPlanNodeFactoryMethodWithAllValidSortFunctions() {
        String[] validSortFunctions = { "avg", "max", "sum" };

        for (String sortFunction : validSortFunctions) {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setFunctionName("sort");
            functionNode.addChildNode(new ValueNode(sortFunction));

            SortPlanNode node = SortPlanNode.of(functionNode);
            assertEquals(sortFunction, node.getSortBy());
            assertEquals("desc", node.getSortOrder());
        }
    }

    public void testSortPlanNodeFactoryMethodWithAllValidSortOrders() {
        String[] validOrders = { "asc", "desc" };

        for (String order : validOrders) {
            FunctionNode functionNode = new FunctionNode();
            functionNode.setFunctionName("sort");
            functionNode.addChildNode(new ValueNode("avg"));
            functionNode.addChildNode(new ValueNode(order));

            SortPlanNode node = SortPlanNode.of(functionNode);
            assertEquals("avg", node.getSortBy());
            assertEquals(order, node.getSortOrder());
        }
    }

    public void testSortPlanNodeFactoryMethodThrowsOnNoArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnInvalidSortFunction() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("invalid"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnInvalidSortOrder() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("invalid"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnTooManyArguments() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new ValueNode("avg"));
        functionNode.addChildNode(new ValueNode("desc"));
        functionNode.addChildNode(new ValueNode("extra"));

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    public void testSortPlanNodeFactoryMethodThrowsOnNonValueNode() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("sort");
        functionNode.addChildNode(new FunctionNode()); // not a value node

        expectThrows(IllegalArgumentException.class, () -> SortPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(SortPlanNode planNode) {
            return "visit SortPlanNode";
        }
    }
}
