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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.chunk.XORIterator;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ChunkEncodingBenchmark {

    private static final long MIN_TIME = 123450000;
    private static final long STEP = 10000;

    @Param({ "500" })
    public int samplesPerSeries;

    @Param({ "1000" })
    public int numSeries;

    private SampleList[] decoded;
    private byte[][] encoded;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        Random random = new Random(12345);
        decoded = new SampleList[numSeries];
        encoded = new byte[numSeries][];

        for (int i = 0; i < numSeries; i++) {
            FloatSampleList.Builder builder = new FloatSampleList.Builder(samplesPerSeries);
            for (int j = 0; j < samplesPerSeries; j++) {
                builder.add(MIN_TIME + j * STEP, random.nextDouble());
            }
            decoded[i] = builder.build();
            encoded[i] = encode(decoded[i]);
        }
    }

    @Benchmark
    public void benchEncode(Blackhole bh) {
        for (int i = 0; i < numSeries; i++) {
            bh.consume(encode(decoded[i]));
        }
    }

    @Benchmark
    public void benchDecode(Blackhole bh) {
        for (int i = 0; i < numSeries; i++) {
            bh.consume(decode(encoded[i]));
        }
    }

    private byte[] encode(SampleList samples) {
        XORChunk chunk = new XORChunk();
        ChunkAppender appender = chunk.appender();
        for (Sample s : samples) {
            appender.append(s.getTimestamp(), s.getValue());
        }
        return chunk.bytes();
    }

    private SampleList decode(byte[] raw) {
        XORIterator xorIterator = new XORIterator(raw);
        return xorIterator.decodeSamples(MIN_TIME, MIN_TIME + STEP * samplesPerSeries).samples();
    }
}
