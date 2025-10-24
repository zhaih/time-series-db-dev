/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

import java.util.Locale;

/**
 * TagValueNodes represent the raw value of a tag in a M3QL query. These are special nodes that may be post processed into a TagValueNode,
 * or expanded to an TagArgsNode if they contain brace interpolation patterns.
 */
public class TagValueNode extends M3ASTNode {
    private final String value;

    /**
     * Constructor for TagValueNode.
     * @param value the value represented by this node
     */
    public TagValueNode(String value) {
        this.value = value;
    }

    /**
     * Get the value represented by this node.
     * @return the value
     */
    public String getValue() {
        return value;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "TAG_VALUE(%s)", value);
    }
}
