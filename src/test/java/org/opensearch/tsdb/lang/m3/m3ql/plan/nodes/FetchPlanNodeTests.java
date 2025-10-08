/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagKeyNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for FetchPlanNode.
 */
public class FetchPlanNodeTests extends BasePlanNodeTests {

    public void testFetchPlanNodeCreation() {
        Map<String, List<String>> matchFilters = new HashMap<>();
        matchFilters.put("host", Arrays.asList("server1", "server2"));
        Map<String, List<String>> inverseMatchFilters = new HashMap<>();
        inverseMatchFilters.put("env", Collections.singletonList("test"));

        FetchPlanNode node = new FetchPlanNode(1, matchFilters, inverseMatchFilters);

        assertEquals(1, node.getId());
        assertEquals(matchFilters, node.getMatchFilters());
        assertEquals(inverseMatchFilters, node.getInverseMatchFilters());
        assertTrue(node.getExplainName().contains("FETCH"));
        assertTrue(node.getChildren().isEmpty());
    }

    public void testFetchPlanNodeWithEmptyFilters() {
        Map<String, List<String>> emptyFilters = new HashMap<>();
        FetchPlanNode node = new FetchPlanNode(1, emptyFilters, emptyFilters);

        assertTrue(node.getMatchFilters().isEmpty());
        assertTrue(node.getInverseMatchFilters().isEmpty());
    }

    public void testFetchPlanNodeVisitorAccept() {
        Map<String, List<String>> filters = new HashMap<>();
        FetchPlanNode node = new FetchPlanNode(1, filters, filters);
        TestMockVisitor visitor = new TestMockVisitor();

        String result = node.accept(visitor);
        assertEquals("visit FetchPlanNode", result);
    }

    public void testFetchPlanNodeFactoryMethodWithSingleValue() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("host");
        tagKey.addChildNode(new ValueNode("server1"));
        functionNode.addChildNode(tagKey);

        FetchPlanNode node = FetchPlanNode.of(functionNode);

        assertFalse(node.getMatchFilters().isEmpty());
        assertTrue(node.getInverseMatchFilters().isEmpty());
        assertEquals(Collections.singletonList("server1"), node.getMatchFilters().get("host"));
    }

    public void testFetchPlanNodeFactoryMethodWithMultipleValues() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("service");
        ArgsNode args = new ArgsNode();
        args.addArg("web");
        args.addArg("api");
        tagKey.addChildNode(args);
        functionNode.addChildNode(tagKey);

        FetchPlanNode node = FetchPlanNode.of(functionNode);

        assertEquals(Arrays.asList("web", "api"), node.getMatchFilters().get("service"));
    }

    public void testFetchPlanNodeFactoryMethodWithInvertedFilter() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("env");
        tagKey.setInverted(true);
        tagKey.addChildNode(new ValueNode("test"));
        functionNode.addChildNode(tagKey);

        FetchPlanNode node = FetchPlanNode.of(functionNode);

        assertTrue(node.getMatchFilters().isEmpty());
        assertEquals(Collections.singletonList("test"), node.getInverseMatchFilters().get("env"));
    }

    public void testFetchPlanNodeFactoryMethodThrowsOnNullFunction() {
        expectThrows(IllegalArgumentException.class, () -> FetchPlanNode.of(null));
    }

    public void testFetchPlanNodeFactoryMethodThrowsOnNonTagKeyChild() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        functionNode.addChildNode(new ValueNode("not_tag_key"));

        expectThrows(IllegalArgumentException.class, () -> FetchPlanNode.of(functionNode));
    }

    public void testFetchPlanNodeFactoryMethodThrowsOnEmptyKeyName() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.setKeyName("");
        tagKey.addChildNode(new ValueNode("value"));
        functionNode.addChildNode(tagKey);

        expectThrows(IllegalArgumentException.class, () -> FetchPlanNode.of(functionNode));
    }

    public void testFetchPlanNodeFactoryMethodThrowsOnNullKeyName() {
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagKeyNode tagKey = new TagKeyNode();
        tagKey.addChildNode(new ValueNode("value"));
        functionNode.addChildNode(tagKey);

        expectThrows(IllegalArgumentException.class, () -> FetchPlanNode.of(functionNode));
    }

    private static class TestMockVisitor extends M3PlanVisitor<String> {
        @Override
        public String process(M3PlanNode planNode) {
            return "process called";
        }

        @Override
        public String visit(FetchPlanNode planNode) {
            return "visit FetchPlanNode";
        }
    }
}
