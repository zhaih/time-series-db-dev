/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregator;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregatorFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for TimeSeriesUnfoldAggregator measuring collection and reduction performance.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class TimeSeriesUnfoldAggregationBenchmark extends BaseTSDBBenchmark {
    @Param({ "10", "100", "1000", "10000" })
    public int cardinality;

    @Param({ "10", "100" })
    public int sampleCount;

    @Param({ "10", "20" })
    public int labelCount;

    @Param({ "scale" })
    public String stageType;

    private TimeSeriesUnfoldAggregator aggregator;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        setupBenchmark(this.cardinality, this.sampleCount, this.labelCount);
    }

    @Setup(Level.Invocation)
    public void setupAggregator() throws IOException {
        UnaryPipelineStage stage = createStage();
        TimeSeriesUnfoldAggregatorFactory factory = new TimeSeriesUnfoldAggregatorFactory(
            "unfold_scale",
            searchContext.getQueryShardContext(),
            null,
            AggregatorFactories.builder(),
            Collections.emptyMap(),
            List.of(stage),
            MIN_TS,
            maxTs,
            STEP
        );

        aggregator = (TimeSeriesUnfoldAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.ONE,
            Collections.emptyMap()
        );
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        tearDownBenchmark();
    }

    private UnaryPipelineStage createStage() {
        switch (stageType) {
            case "scale":
            default:
                return new ScaleStage(2.0);
        }
    }

    /**
     * Benchmarks the complete aggregation collection and reduction cycle for time series data.
     * Measures: preCollection -> search -> postCollection -> buildTopLevel -> reduction
     */
    @Benchmark
    public void benchmarkCollectWithAggregator(Blackhole bh) throws IOException {
        aggregator.preCollection();
        indexSearcher.search(rewritten, aggregator);
        aggregator.postCollection();

        InternalAggregation reduced = finalizeReduction();
        bh.consume(reduced);
    }

    private InternalAggregation.ReduceContext createReduceContext(TimeSeriesUnfoldAggregator aggregator) {
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            DEFAULT_MAX_BUCKETS,
            circuitBreakerService.getBreaker(CircuitBreaker.REQUEST)
        );
        return InternalAggregation.ReduceContext.forFinalReduction(
            aggregator.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineAggregator.PipelineTree.EMPTY
        );
    }

    private InternalAggregation finalizeReduction() throws IOException {
        InternalAggregation result = aggregator.buildTopLevel();
        InternalAggregation.ReduceContext context = createReduceContext(aggregator);

        @SuppressWarnings("unchecked")
        InternalAggregation reduced = result.reduce(List.of(result), context);
        reduced = reduced.reducePipelines(reduced, context, PipelineAggregator.PipelineTree.EMPTY);
        for (PipelineAggregator pipelineAggregator : PipelineAggregator.PipelineTree.EMPTY.aggregators()) {
            reduced = pipelineAggregator.reduce(reduced, context);
        }

        return reduced;
    }

}
