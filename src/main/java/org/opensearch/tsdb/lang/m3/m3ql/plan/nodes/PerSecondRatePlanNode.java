/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.util.Locale;

/**
 * PerSecondRatePlanNode represents a plan node that handles perSecondRate operations in M3QL.
 * Converts a monotonic, increasing timeseries into per-second average rate.
 */
public class PerSecondRatePlanNode extends M3PlanNode {

    private final String interval; // e.g., "10s", "1m"

    /**
     * Constructor for PerSecondRatePlanNode.
     * @param id node id
     * @param interval the lookback interval for rate calculation (e.g., "10s" for 10 seconds)
     */
    public PerSecondRatePlanNode(int id, String interval) {
        super(id);
        this.interval = interval;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "PER_SECOND_RATE(%s)", interval);
    }

    /**
     * Returns the interval as a Duration.
     * @return Duration of the interval
     * @throws IllegalArgumentException if the interval is negative
     */
    public Duration getInterval() {
        Duration parsedInterval = M3Duration.valueOf(interval);
        if (parsedInterval.isNegative()) {
            throw new IllegalArgumentException("Interval cannot be negative: " + interval);
        }
        return parsedInterval;
    }

    /**
     * Returns the raw interval string.
     * @return String interval value
     */
    public String getIntervalString() {
        return interval;
    }

    /**
     * Factory method to create a PerSecondRatePlanNode from a FunctionNode.
     * Expects the function node to represent a perSecondRate function with exactly one argument: interval.
     *
     * @param functionNode the function node representing the perSecondRate function
     * @return a new PerSecondRatePlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly one argument or if the argument is not valid
     */
    public static PerSecondRatePlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().size() != 1) {
            throw new IllegalArgumentException(
                "perSecondRate function must have exactly one argument: interval. Got: " + functionNode.getChildren().size()
            );
        }

        if (!(functionNode.getChildren().getFirst() instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument must be a value representing the interval");
        }

        String interval = valueNode.getValue();
        return new PerSecondRatePlanNode(M3PlannerContext.generateId(), interval);
    }
}
