/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for IntersectPlanNode.
 */
public class IntersectPlanNodeTests extends BinaryPlanNodeTests {

    @Override
    protected BinaryPlanNode getBinaryPlanNode() {
        return new IntersectPlanNode(1, Collections.emptyList());
    }

    public void testIntersectPlanNodeWithNoTags() {
        verifyPlanNodeName("INTERSECT(tags=[])");
        verifyVisitorAccept();
    }

    public void testIntersectPlanNodeWithTags() {
        IntersectPlanNode nodeWithTags = new IntersectPlanNode(2, Arrays.asList("service", "region"));
        assertEquals("INTERSECT(tags=[service, region])", nodeWithTags.getExplainName());
    }

    public void testIntersectPlanNodeDefaultConstructor() {
        IntersectPlanNode node = new IntersectPlanNode(3);
        assertEquals("INTERSECT(tags=[])", node.getExplainName());
        assertTrue(node.getTags().isEmpty());
    }
}
