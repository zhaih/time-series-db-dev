/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan;

/**
 * M3PlannerContext is a context for the M3QL planner that holds the state of the planning process. It also simplifies
 * plan node generation by providing a unique ID generator for plan nodes.
 */
public class M3PlannerContext implements AutoCloseable {
    private static final ThreadLocal<M3PlannerContext> CONTEXT = ThreadLocal.withInitial(() -> null);

    private final IdGenerator idGenerator = new IdGenerator();

    /**
     * Private constructor to prevent direct instantiation.
     */
    private M3PlannerContext() {
        // Private constructor to prevent direct instantiation
    }

    /**
     * Generate a unique ID for a plan node.
     * @return a unique integer ID
     */
    public static int generateId() {
        return CONTEXT.get().idGenerator.nextId();
    }

    /**
     * Create a new planner context
     * @return a new M3PlannerContext instance
     */
    public static M3PlannerContext create() {
        M3PlannerContext context = new M3PlannerContext();
        CONTEXT.set(context);
        return context;
    }

    /**
     * Close the planner context.
     */
    @Override
    public void close() throws Exception {
        CONTEXT.remove();
    }

    private static class IdGenerator {
        private int counter = 0;

        public int nextId() {
            return counter++;
        }
    }
}
