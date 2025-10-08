/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;

public class BasePlanNodeTests extends OpenSearchTestCase {
    private M3PlannerContext context;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = M3PlannerContext.create();
    }

    @Override
    public void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
        super.tearDown();
    }
}
