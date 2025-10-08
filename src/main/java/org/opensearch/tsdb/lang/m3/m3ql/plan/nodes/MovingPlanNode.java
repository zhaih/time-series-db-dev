/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.time.Duration;
import java.util.Locale;

/**
 * MovingPlanNode represents a plan node that handles moving window operations in M3QL.
 */
public class MovingPlanNode extends M3PlanNode {

    private final String windowSize; // 2h, 5m etc.
    private final AggregationType aggregationType;

    /**
     * Constructor for MovingPlanNode.
     * @param id node id
     * @param windowSize the size of the moving window (e.g., "5m" for 5 minutes or "10" for 10 points)
     * @param aggregationType the type of aggregation to perform over the moving window
     */
    public MovingPlanNode(int id, String windowSize, AggregationType aggregationType) {
        super(id);
        this.windowSize = windowSize;
        this.aggregationType = aggregationType;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.getDefault(), "MOVING(%s, %s)", windowSize, aggregationType);
    }

    /**
     * Returns the duration of the window as time interval, expects format like "1d", "2h", etc.
     * @return Duration
     */
    public Duration getTimeDuration() {
        return M3Duration.valueOf(windowSize);
    }

    /**
     * Returns the duration of the window as number of points.
     * @return Integer number of points
     */
    public Integer getPointDuration() {
        return Integer.parseInt(windowSize);
    }

    /**
     * Returns true if the moving window is point based.
     * @return boolean true if point based, false if time based
     */
    public boolean isPointBased() {
        return windowSize != null && windowSize.trim().matches("\\d+");
    }

    /**
     * Returns the aggregation type for the moving window operation.
     * @return AggregationType
     */
    public AggregationType getAggregationType() {
        return aggregationType;
    }

    /**
     * Factory method to create a MovingPlanNode from a FunctionNode.
     * Expects the function node to represent a MOVING function with exactly two arguments:
     * window size and aggregation type.
     *
     * @param functionNode the function node representing the MOVING function
     * @return a new MovingPlanNode instance
     * @throws IllegalArgumentException if the function node does not have exactly two arguments or if the arguments are not valid
     */
    public static MovingPlanNode of(FunctionNode functionNode) {
        if (functionNode.getChildren().size() != 2) {
            throw new IllegalArgumentException("Moving function must have exactly two arguments: window size and aggregation type");
        }
        if (!(functionNode.getChildren().get(0) instanceof ValueNode firstValueNode)) {
            throw new IllegalArgumentException("First argument must be a value representing the windowSize");
        }
        if (!(functionNode.getChildren().get(1) instanceof ValueNode secondValueNode)) {
            throw new IllegalArgumentException("Second argument must be a value representing the aggregation type");
        }

        String windowSize = firstValueNode.getValue();
        String aggregationType = secondValueNode.getValue();

        return new MovingPlanNode(M3PlannerContext.generateId(), windowSize, AggregationType.fromString(aggregationType));
    }
}
