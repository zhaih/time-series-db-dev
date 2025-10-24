/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagArgsNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.TagValueNode;

import java.util.ArrayList;
import java.util.List;

/**
 * AST transformation that expands TagValueNodes containing brace interpolation patterns
 * into ArgsNodes with multiple TagValueNodes.
 *
 * Examples:
 * - name:abc => VALUE(abc) (no change)
 * - name:aaa{b,c,d}eee => ARGS({aaabeee,aaaceee,aaadeee})
 * - name:{aaa{b,c}e,f}ee => ARGS({aaabeee,aaaceee,fee})
 */
public class ValueInterpolationTransformation implements ASTTransformation {

    /**
     * Constructor for ValueInterpolationTransformation.
     */
    public ValueInterpolationTransformation() {}

    @Override
    public boolean canTransform(M3ASTNode node) {
        if (!(node instanceof TagValueNode tagValueNode)) {
            return false;
        }
        return containsBraces(tagValueNode.getValue());
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        if (!(node instanceof TagValueNode tagValueNode)) {
            throw new IllegalArgumentException("Expected TagValueNode for interpolation");
        }

        String value = tagValueNode.getValue();

        List<String> expandedValues = expandBracePattern(value);

        if (expandedValues.size() == 1) {
            // Only one value after expansion, return a value node
            return List.of(new TagValueNode(expandedValues.getFirst()));
        }

        // Create ArgsNode with expanded values
        TagArgsNode argsNode = new TagArgsNode();
        for (String expandedValue : expandedValues) {
            argsNode.addArg(expandedValue);
        }

        return List.of(argsNode);
    }

    private boolean containsBraces(String value) {
        int braceCount = 0;
        boolean hasOpenBrace = false;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '{') {
                braceCount++;
                hasOpenBrace = true;
            } else if (c == '}') {
                braceCount--;
                if (braceCount < 0) {
                    throw new IllegalArgumentException("Mismatched braces: unopened closing brace at position " + i);
                }
            }
        }

        if (braceCount > 0) {
            throw new IllegalArgumentException("Mismatched braces: " + braceCount + " unclosed opening brace(s)");
        }

        return hasOpenBrace;
    }

    private List<String> expandBracePattern(String value) {
        List<String> result = new ArrayList<>();
        result.add(value);

        // Keep expanding until no more braces remain
        boolean hasChanges = true;
        while (hasChanges) {
            hasChanges = false;
            List<String> nextResult = new ArrayList<>();

            for (String current : result) {
                if (!containsBraces(current)) {
                    // No braces in this string, keep as-is
                    nextResult.add(current);
                } else {
                    // Has braces, try to expand
                    List<String> expanded = expandFirstBrace(current);
                    assert !expanded.isEmpty() : "expandFirstBrace will never return empty list";
                    hasChanges = true;
                    nextResult.addAll(expanded);
                }
            }
            result = nextResult;
        }

        return result;
    }

    private List<String> expandFirstBrace(String value) {
        int[] braceIndexes = findFirstBracePair(value);
        if (braceIndexes[0] == -1) {
            // if canTransform is used correctly, this will not be reached
            throw new IllegalStateException("Cannot expand value without braces: " + value);
        }

        int openBrace = braceIndexes[0];
        int closeBrace = braceIndexes[1];

        String prefix = value.substring(0, openBrace);
        String suffix = value.substring(closeBrace + 1);
        String braceContent = value.substring(openBrace + 1, closeBrace);

        List<String> options = splitByComma(braceContent);
        List<String> result = new ArrayList<>();

        for (String option : options) {
            String expanded = prefix + option.trim() + suffix;
            result.add(expanded);
        }

        return result;
    }

    private int[] findFirstBracePair(String value) {
        int openBrace = -1;
        int braceCount = 0;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '{') {
                if (openBrace == -1) {
                    openBrace = i;
                }
                braceCount++;
            } else if (c == '}') {
                braceCount--;
                if (braceCount == 0 && openBrace != -1) {
                    return new int[] { openBrace, i };
                }
            }
        }
        // if canTransform is used correctly, this will not be reached
        throw new IllegalStateException("Cannot find braces in value without braces: " + value);
    }

    private List<String> splitByComma(String content) {
        List<String> result = new ArrayList<>();
        if (content.isEmpty()) {
            result.add("");
            return result;
        }

        int start = 0;
        int braceCount = 0;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                braceCount++;
            } else if (c == '}') {
                braceCount--;
            } else if (c == ',' && braceCount == 0) {
                result.add(content.substring(start, i));
                start = i + 1;
            }
        }
        result.add(content.substring(start));

        return result;
    }
}
