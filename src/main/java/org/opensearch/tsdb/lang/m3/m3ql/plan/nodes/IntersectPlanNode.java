/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Plan node for the intersect function that filters series by matching labels with a subquery.
 */
public class IntersectPlanNode extends BinaryByTagsPlanNode {

    /**
     * Constructs an IntersectPlanNode with the specified id and tags.
     *
     * @param id   the unique identifier for this plan node
     * @param tags the list of tag names to use for matching; if empty, all tags are used
     */
    public IntersectPlanNode(int id, List<String> tags) {
        super(id, tags);
    }

    /**
     * Constructs an IntersectPlanNode with the specified id and no specific tags (match all).
     *
     * @param id the unique identifier for this plan node
     */
    public IntersectPlanNode(int id) {
        this(id, Collections.emptyList());
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "INTERSECT(tags=%s)", this.getTags());
    }
}
