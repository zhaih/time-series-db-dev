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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for AliasByTagsPlanNode.
 */
public class AliasByTagsPlanNodeTests extends BasePlanNodeTests {

    public void testAliasByTagsPlanNodeCreation() {
        List<String> tagNames = Arrays.asList("host", "service");
        AliasByTagsPlanNode node = new AliasByTagsPlanNode(1, tagNames);

        assertEquals(1, node.getId());
        assertEquals(tagNames, node.getTagNames());
        assertEquals("ALIAS_BY_TAGS(host, service)", node.getExplainName());
        assertTrue(node.getChildren().isEmpty());
    }

    public void testAliasByTagsPlanNodeWithSingleTag() {
        List<String> tagNames = Collections.singletonList("host");
        AliasByTagsPlanNode node = new AliasByTagsPlanNode(1, tagNames);

        assertEquals(tagNames, node.getTagNames());
        assertEquals("ALIAS_BY_TAGS(host)", node.getExplainName());
    }

    public void testAliasByTagsPlanNodeWithEmptyTags() {
        List<String> tagNames = Collections.emptyList();
        AliasByTagsPlanNode node = new AliasByTagsPlanNode(1, tagNames);

        assertEquals(tagNames, node.getTagNames());
        assertEquals("ALIAS_BY_TAGS()", node.getExplainName());
    }

    public void testAliasByTagsPlanNodeVisitorAccept() {
        AliasByTagsPlanNode node = new AliasByTagsPlanNode(1, Arrays.asList("tag1"));
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit AliasByTagsPlanNode", result);
    }

    public void testAliasByTagsPlanNodeFactoryMethod() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("aliasByTags");
        functionNode.addChildNode(new ValueNode("\"host\""));
        functionNode.addChildNode(new ValueNode("\"service\""));

        AliasByTagsPlanNode node = AliasByTagsPlanNode.of(functionNode);

        assertEquals(Arrays.asList("host", "service"), node.getTagNames());
    }

    public void testAliasByTagsPlanNodeFactoryMethodWithQuotedValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("aliasByTags");
        functionNode.addChildNode(new ValueNode("\"environment\""));
        functionNode.addChildNode(new ValueNode("\"region\""));

        AliasByTagsPlanNode node = AliasByTagsPlanNode.of(functionNode);

        assertEquals(Arrays.asList("environment", "region"), node.getTagNames());
    }

    public void testAliasByTagsPlanNodeFactoryMethodWithNoChildren() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("aliasByTags");

        AliasByTagsPlanNode node = AliasByTagsPlanNode.of(functionNode);

        assertTrue(node.getTagNames().isEmpty());
    }

    public void testGetTagNamesReturnsDefensiveCopy() {
        List<String> originalTags = Arrays.asList("host", "service");
        AliasByTagsPlanNode node = new AliasByTagsPlanNode(1, originalTags);

        List<String> returnedTags = node.getTagNames();
        returnedTags.add("newTag");

        assertEquals(2, node.getTagNames().size());
        assertFalse(node.getTagNames().contains("newTag"));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(AliasByTagsPlanNode planNode) {
            return "visit AliasByTagsPlanNode";
        }
    }
}
