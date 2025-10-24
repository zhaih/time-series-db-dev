/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;

import java.util.List;

public class ValueInterpolationTransformationTests extends OpenSearchTestCase {

    public void testCanTransformReturnsFalseForNonTagValueNode() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");

        assertFalse("Should not transform non-TagValueNode", transformation.canTransform(functionNode));
    }

    public void testCanTransformReturnsFalseForTagValueNodeWithoutBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("abc");

        assertFalse("Should not transform TagValueNode without braces", transformation.canTransform(valueNode));
    }

    public void testCanTransformReturnsTrueForTagValueNodeWithBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("aaa{b,c,d}eee");

        assertTrue("Should transform TagValueNode with braces", transformation.canTransform(valueNode));
    }

    public void testTransformSimpleValueWithoutBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("abc");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single node for value without braces", 1, results.size());
        assertTrue("Result should be a TagValueNode", results.getFirst() instanceof TagValueNode);
        assertEquals("abc", ((TagValueNode) results.getFirst()).getValue());
    }

    public void testTransformSimpleBraceExpansion() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("aaa{b,c,d}eee");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagArgsNode", 1, results.size());
        assertTrue("Result should be TagArgsNode", results.getFirst() instanceof TagArgsNode);

        TagArgsNode argsNode = (TagArgsNode) results.getFirst();
        List<String> args = argsNode.getArgs();
        assertEquals("Should have 3 expanded values", 3, args.size());
        assertEquals("aaabeee", args.get(0));
        assertEquals("aaaceee", args.get(1));
        assertEquals("aaadeee", args.get(2));
    }

    public void testTransformNestedBraceExpansion() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("{aaa{b,c}e,f}ee");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagArgsNode", 1, results.size());
        assertTrue("Result should be TagArgsNode", results.getFirst() instanceof TagArgsNode);

        TagArgsNode argsNode = (TagArgsNode) results.getFirst();
        List<String> args = argsNode.getArgs();
        assertEquals("Should have 3 expanded values", 3, args.size());
        assertEquals("aaabeee", args.get(0));
        assertEquals("aaaceee", args.get(1));
        assertEquals("fee", args.get(2));
    }

    public void testTransformMultipleBraceGroups() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("prefix{a,b}middle{x,y}suffix");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagArgsNode", 1, results.size());
        assertTrue("Result should be TagArgsNode", results.getFirst() instanceof TagArgsNode);

        TagArgsNode argsNode = (TagArgsNode) results.getFirst();
        List<String> args = argsNode.getArgs();
        assertEquals("Should have 4 expanded values", 4, args.size());
        assertEquals("prefixamiddlexsuffix", args.get(0));
        assertEquals("prefixamiddleysuffix", args.get(1));
        assertEquals("prefixbmiddlexsuffix", args.get(2));
        assertEquals("prefixbmiddleysuffix", args.get(3));
    }

    public void testTransformWithEmptyBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("name{}end");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagValueNode", 1, results.size());
        assertTrue("Result should be TagValueNode", results.getFirst() instanceof TagValueNode);

        TagValueNode output = (TagValueNode) results.getFirst();
        assertEquals("nameend", output.getValue());
    }

    public void testTransformWithSingleValueInBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("a{single}b");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagValueNode", 1, results.size());
        assertTrue("Result should be TagValueNode", results.getFirst() instanceof TagValueNode);

        TagValueNode output = (TagValueNode) results.getFirst();
        assertEquals("asingleb", output.getValue());
    }

    public void testTransformComplexNestedCase() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("{a{1,2}b,c{3,4}}end");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagArgsNode", 1, results.size());
        assertTrue("Result should be TagArgsNode", results.getFirst() instanceof TagArgsNode);

        TagArgsNode argsNode = (TagArgsNode) results.getFirst();
        List<String> args = argsNode.getArgs();
        assertEquals("Should have 4 expanded values", 4, args.size());
        assertEquals("a1bend", args.get(0));
        assertEquals("a2bend", args.get(1));
        assertEquals("c3end", args.get(2));
        assertEquals("c4end", args.get(3));
    }

    public void testTransformWithASTTransformer() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();

        // Create a function node with a TagValueNode child that needs interpolation
        FunctionNode functionNode = new FunctionNode();
        functionNode.setFunctionName("fetch");
        TagValueNode valueNode = new TagValueNode("metric{a,b,c}");
        functionNode.addChildNode(valueNode);

        // Apply transformation using ASTTransformer
        ASTTransformer transformer = new ASTTransformer(List.of(transformation));
        transformer.transform(functionNode);

        // Verify the transformation results
        assertEquals("Function should still have 1 child", 1, functionNode.getChildren().size());
        M3ASTNode transformedChild = functionNode.getChildren().get(0);
        assertTrue("Child should be transformed to TagArgsNode", transformedChild instanceof TagArgsNode);

        TagArgsNode argsNode = (TagArgsNode) transformedChild;
        List<String> args = argsNode.getArgs();
        assertEquals("Should have 3 expanded values", 3, args.size());
        assertEquals("metrica", args.get(0));
        assertEquals("metricb", args.get(1));
        assertEquals("metricc", args.get(2));
    }

    public void testNestedBracesNoExpansion() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();
        TagValueNode valueNode = new TagValueNode("{{{{{nested}}}}}");

        List<M3ASTNode> results = transformation.transform(valueNode);

        assertEquals("Should return single TagValueNode", 1, results.size());
        assertTrue("Result should be TagValueNode", results.getFirst() instanceof TagValueNode);

        TagValueNode output = (TagValueNode) results.getFirst();
        assertEquals("nested", output.getValue());
    }

    public void testMismatchedBraces() {
        ValueInterpolationTransformation transformation = new ValueInterpolationTransformation();

        // Test unclosed brace
        TagValueNode valueNode1 = new TagValueNode("prefix{a,b");
        Exception exception1 = assertThrows(IllegalArgumentException.class, () -> { transformation.transform(valueNode1); });
        assertTrue(
            "Exception should mention mismatched braces",
            exception1.getMessage().contains("mismatched") || exception1.getMessage().contains("unclosed")
        );

        // Test unopened brace
        TagValueNode valueNode2 = new TagValueNode("prefix}a,b");
        Exception exception2 = assertThrows(IllegalArgumentException.class, () -> { transformation.transform(valueNode2); });
        assertTrue(
            "Exception should mention mismatched braces",
            exception2.getMessage().contains("mismatched") || exception2.getMessage().contains("unopened")
        );

        // Test mixed mismatched braces
        TagValueNode valueNode3 = new TagValueNode("prefix{a{b}");
        Exception exception3 = assertThrows(IllegalArgumentException.class, () -> { transformation.transform(valueNode3); });
        assertTrue(
            "Exception should mention mismatched braces",
            exception3.getMessage().contains("mismatched") || exception3.getMessage().contains("unclosed")
        );
    }
}
