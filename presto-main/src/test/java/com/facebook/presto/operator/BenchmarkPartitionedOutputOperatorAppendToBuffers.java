/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.BasicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperatorAppendToBuffers
{
    private static final int RUNS = 1000;
//
    @Benchmark
    public void addLongValuesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.getPageCount();
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
                    boolean[] nulls = data.getNulls(pageIndex, channelIndex);
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putLongValuesWithNullsToBuffer(
                                    longValues,
                                    nulls,
                                    data.getPositions(partition),
                                    0,
                                    data.getPostistionCount(partition),
                                    0,
                                    data.getLongValueBuffer(partition, channelIndex),
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putLongValuesToBuffer(
                                    longValues,
                                    data.getPositions(partition),
                                    0,
                                    data.getPostistionCount(partition),
                                    0,
                                    data.getLongValueBuffer(partition, channelIndex),
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

//    @Benchmark
//    public void addLongValuesNotNullToByteArrayBuffer(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            for (int pageIndex = 0; pageIndex < data.getPageCount(); pageIndex++) {
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putLongValuesToBuffer(
//                                longValues,
//                                data.getPositions(partition),
//                                0,
//                                data.getPostistionCount(partition),
//                                0,
//                                data.getLongValueBuffer(partition, channelIndex),
//                                longValueBufferIndexes[partition][channelIndex]);
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addLongValuesToByteArrayBuffer2(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                        long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                        longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putLongValuesToBuffer(
//                                longValues,
//                                data.getPositions(partition),
//                                0,
//                                data.getPostistionCount(partition),
//                                0,
//                                data.getLongValueBuffer(partition, channelIndex),
//                                longValueBufferIndexes[partition][channelIndex]);
//                    }
//                }
//            }
//        }
//    }

//    @Benchmark
//    public void addLongPagesToByteArrayBuffer(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getBigintDataPage(pageIndex);
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    Block bigintBlock = page.getBlock(channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        int[] positions = data.getPositions(partition);
//                        int positionCount = data.getPostistionCount(partition);
//
//                        if (data.mayHaveNull) {
//                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockDataToBuffer(
//                                    bigintBlock,
//                                    positions,
//                                    positionCount,
//                                    data.getLongValueBuffer(partition, channelIndex),
//                                    longValueBufferIndexes[partition][channelIndex]);
//                        }
//                        else {
//                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockDataToBuffer(
//                                    bigintBlock,
//                                    positions,
//                                    positionCount,
//                                    data.getLongValueBuffer(partition, channelIndex),
//                                    longValueBufferIndexes[partition][channelIndex]);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addLongValuesToDynamicSliceOutput(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            data.resetSliceOutput();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                    boolean[] nulls = data.getNulls(pageIndex, channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        SliceOutput sliceOutput = data.sliceOutput[partition][channelIndex];
//                        if (data.mayHaveNull) {
//                            putLongValuesWithNullsToSlice(
//                                    longValues,
//                                    nulls,
//                                    data.getPositions(partition),
//                                    0,
//                                    data.getPostistionCount(partition),
//                                    0,
//                                    sliceOutput);
//                        }
//                        else {
//                            putLongValuesToSlice(
//                                    longValues,
//                                    data.getPositions(partition),
//                                    0,
//                                    data.getPostistionCount(partition),
//                                    0,
//                                    sliceOutput);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addLongValuesToOptimizedSliceOutput(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            data.resetOptimizedSliceOutput();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                    boolean[] nulls = data.getNulls(pageIndex, channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        OptimizedSliceOutput sliceOutput = data.optimizedSliceOutput[partition][channelIndex];
//                        if (data.mayHaveNull) {
//                            putLongValuesWithNullsToSlice2(
//                                    longValues,
//                                    nulls,
//                                    data.getPositions(partition),
//                                    0,
//                                    data.getPostistionCount(partition),
//                                    0,
//                                    sliceOutput);
//                        }
//                        else {
//                            putLongValuesToSlice(
//                                    longValues,
//                                    data.getPositions(partition),
//                                    0,
//                                    data.getPostistionCount(partition),
//                                    0,
//                                    sliceOutput);
//                        }
//                    }
//                }
//            }
//        }
//    }

//    @Benchmark
//    public void addLongPagesToDynamicSliceOutput(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            data.resetSliceOutput();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getBigintDataPage(pageIndex);
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    Block bigintBlock = page.getBlock(channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        int[] positions = data.getPositions(partition);
//                        int positionCount = data.getPostistionCount(partition);
//
//                        SliceOutput sliceOutput = data.sliceOutput[partition][channelIndex];
//                        if (data.mayHaveNull) {
//                            putBigintWithNullsBlockDataToSlice(bigintBlock, positions, positionCount, sliceOutput);
//                        }
//                        else {
//                            putBigintBlockDataToSlice(bigintBlock, positions, positionCount, sliceOutput);
//                        }
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addLongPagesToByteArrayBuffer2(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getBigintDataPage(pageIndex);
//                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                    int[] positions = data.getPositions(partition);
//                    int positionCount = data.getPostistionCount(partition);
//
//                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                        Block bigintBlock = page.getBlock(channelIndex);
//
//                        longValueBufferIndexes[partition][channelIndex] = putBigintBlockDataToBuffer(
//                                bigintBlock,
//                                positions,
//                                positionCount,
//                                data.getLongValueBuffer(partition, channelIndex),
//                                longValueBufferIndexes[partition][channelIndex]);
//                    }
//                }
//            }
//        }
//    }
////
//    @Benchmark
//    public void addLongValuesToByteArrayBufferSimple(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getBigintDataPage(pageIndex);
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                    boolean[] nulls = data.getNulls(pageIndex, channelIndex);
//                    if (data.mayHaveNull) {
//                        ByteArrayUtils.putAllLongValuesWithNullsToBuffer(
//                                longValues,
//                                nulls,
//                                0,
//                                longValues.length,
//                                0,
//                                data.getLongValueBuffer(0, channelIndex),
//                                longValueBufferIndexes[0][channelIndex]);
//                    }
//                    else {
//                        ByteArrayUtils.putAllLongValuesToBuffer(
//                                longValues,
//                                0,
//                                longValues.length,
//                                0,
//                                data.getLongValueBuffer(0, channelIndex),
//                                longValueBufferIndexes[0][channelIndex]);
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addLongPagesToByteArrayBufferSimple(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getBigintDataPage(pageIndex);
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    Block bigintBlock = page.getBlock(channelIndex);
//
//                    if (data.mayHaveNull) {
//                        putAllBigintWithNullsBlockDataToBuffer(
//                                bigintBlock,
//                                data.getLongValueBuffer(0, channelIndex),
//                                longValueBufferIndexes[0][channelIndex]);
//                    }
//                    else {
//                        putAllBigintBlockDataToBuffer(
//                                bigintBlock,
//                                data.getLongValueBuffer(0, channelIndex),
//                                longValueBufferIndexes[0][channelIndex]);
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void benchmarkSetInt(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//
//            int[] offsets = data.intValues;
//            byte[] buffer = data.intBuffer;
//            int index = 0;
//            for (int j = 0; j < offsets.length; j++) {
//                ByteArrayUtils.writeInt(buffer, index, offsets[j]);
//                index += ARRAY_INT_INDEX_SCALE;
//            }
//        }
//    }
//
//    @Benchmark
//    public void benchmarkSetInt2(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//
//            int[] offsets = data.intValues;
//            byte[] buffer = data.intBuffer;
//            int index = ARRAY_BYTE_BASE_OFFSET;
//            for (int j = 0; j < offsets.length; j++) {
//                ByteArrayUtils.setInt2(buffer, index, offsets[j]);
//                index += ARRAY_INT_INDEX_SCALE;
//            }
//        }
//    }

    private int putAllBigintBlockDataToBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLong(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private int putAllBigintWithNullsBlockDataToBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLong(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNull(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

//    @Benchmark
//    public void addCompositeValuesToByteArrayBuffer(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//            int[][] shortValueBufferIndexes = data.getShortValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    long[] longValues = data.getLongValues(pageIndex, channelIndex);
//                    short[] shortValues = data.getShortValues(pageIndex, channelIndex);
//
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        int[] positions = data.getPositions(partition);
//                        int positionCount = data.getPostistionCount(partition);
//
//                        longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putLongValuesToBuffer(
//                                longValues,
//                                positions,
//                                0,
//                                positionCount,
//                                0,
//                                data.getLongValueBuffer(partition, channelIndex),
//                                longValueBufferIndexes[partition][channelIndex]);
//
//                        shortValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.putShortValuesToBuffer(
//                                shortValues,
//                                positions,
//                                0,
//                                positionCount,
//                                0,
//                                data.getShortValueBuffer(partition, channelIndex),
//                                shortValueBufferIndexes[partition][channelIndex]);
//                    }
//                }
//            }
//        }
//    }
//
//    @Benchmark
//    public void addCompositePagesToByteArrayBuffer(BenchmarkData data)
//    {
//        for (int i = 0; i < RUNS; i++) {
//            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
//            int[][] shortValueBufferIndexes = data.getShortValueBufferIndexes();
//
//            int pageCount = data.getPageCount();
//            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
//                Page page = data.getMixedTypeDataPage(pageIndex);
//                int channelCount = data.channelCount;
//                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
//                    Block bigintBlock = page.getBlock(channelIndex);
//                    Block smallintBlock = page.getBlock(channelCount + channelIndex);
//                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
//                        int[] positions = data.getPositions(partition);
//                        int positionCount = data.getPostistionCount(partition);
//
//                        longValueBufferIndexes[partition][channelIndex] = putBigintBlockDataToBuffer(
//                                bigintBlock,
//                                positions,
//                                positionCount,
//                                data.getLongValueBuffer(partition, channelIndex),
//                                longValueBufferIndexes[partition][channelIndex]);
//
//                        shortValueBufferIndexes[partition][channelIndex] = putSmallintBlockDataToBuffer(
//                                smallintBlock,
//                                positions,
//                                positionCount,
//                                data.getShortValueBuffer(partition, channelIndex),
//                                shortValueBufferIndexes[partition][channelIndex]);
//                    }
//                }
//            }
//        }
//    }

    private int putBigintBlockDataToBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        for (int j = 0; j < positionCount; j++) {
            long longValue = block.getLong(positions[j]);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private int putBigintWithNullsBlockDataToBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        for (int j = 0; j < positionCount; j++) {
            int position = positions[j];
            long longValue = block.getLong(positions[j]);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNull(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

    private int putBigintBlockDataToSlice(Block block, int[] positions, int positionCount, SliceOutput sliceOutput)
    {
        for (int j = 0; j < positionCount; j++) {
            sliceOutput.writeLong(block.getLong(positions[j]));
        }

        return sliceOutput.size();
    }

    private int putBigintWithNullsBlockDataToSlice(Block block, int[] positions, int positionCount, SliceOutput sliceOutput)
    {
        for (int j = 0; j < positionCount; j++) {
            int position = positions[j];
            long value = block.getLong(position);
            sliceOutput.writeLong(value);
            if (!block.isNull(position)) {
                //sliceOutput.writeLong(block.getLong(position));
                sliceOutput.writeLong(value);
            }
        }

        return sliceOutput.size();
    }

//    private int putSmallintBlockDataToBuffer(Block block, int[] positions, int positionCount, byte[] shortValueBuffer, int shortBufferIndex)
//    {
//        for (int j = 0; j < positionCount; j++) {
//            short shortValue = block.getShort(positions[j]);
//            ByteArrayUtils.setShort(shortValueBuffer, shortBufferIndex, shortValue);
//            shortBufferIndex += ARRAY_SHORT_INDEX_SCALE;
//        }
//        return shortBufferIndex;
//    }

    public static int putLongValuesToSlice(long[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, SliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            sliceOutput.writeLong(values[positions[i] + offsetBase]);
        }
        return sliceOutput.size();
    }

    public static int putLongValuesWithNullsToSlice(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, SliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i] + offsetBase;
            long value = values[positions[i] + offsetBase];
            if (!nulls[position]) {
                sliceOutput.writeLong(value);
            }
        }
        return sliceOutput.size();
    }

    public static int putLongValuesToSlice(long[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, OptimizedSliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            //System.out.println(sliceOutput.size());
            sliceOutput.writeLong(values[positions[i] + offsetBase]);
        }
        return sliceOutput.size();
    }

    public static int putLongValuesWithNullsToSlice(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, OptimizedSliceOutput sliceOutput)
    {
        if (sliceOutput.capacity() < batchSize * 8) {
            throw new IllegalArgumentException(format("batchSize %d is too large to fit in buffer %d", batchSize, sliceOutput.capacity()));
        }

        int address = 0;
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i] + offsetBase;
            long value = values[positions[i] + offsetBase];
            sliceOutput.writeLongAt(values[positions[i] + offsetBase], address);
            //sliceOutput.increaseSize();
            if (!nulls[position]) {
                //sliceOutput.size += ARRAY_LONG_INDEX_SCALE;
                address += ARRAY_LONG_INDEX_SCALE;
                //sliceOutput.increaseSize();
            }
            //sliceOutput.writeLongOnCondition(value, !nulls[position]);
        }
        return sliceOutput.size();
    }

    public static void putLongValuesWithNullsToSlice2(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, OptimizedSliceOutput sliceOutput)
    {
        if (sliceOutput.capacity() < batchSize * 8) {
            throw new IllegalArgumentException(format("batchSize %d is too large to fit in buffer %d", batchSize, sliceOutput.capacity()));
        }

//        long[] valuesToCopy = new long[batchSize];
//        boolean[] nullsToCopy = new boolean[batchSize];
//        for (int i = 0; i < batchSize; i++) {
//            int position = positions[i + positionsOffset] + offsetBase;
//            valuesToCopy[i] = values[position];
//            nullsToCopy[i] = nulls[position];
//        }

        for (int i = 0; i < batchSize; i++) {
            int position = positions[i + positionsOffset] + offsetBase;
            values[i] = values[position];
            nulls[i] = nulls[position];
        }

        sliceOutput.writeLongArray(values, nulls, 0, batchSize);
    }

//    public static int putLongValuesWithNullsToSlice(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, SliceOutput sliceOutput)
//    {
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i] + offsetBase;
//
//            if (!nulls[position]) {
//                sliceOutput.writeLong(values[positions[i] + offsetBase]);
//            }
//        }
//        return sliceOutput.size();
//    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int PAGE_COUNT = 4;
        private static final int PARTITION_COUNT = 333;
        private static final int POSITIONS_PER_PAGE = 10000;

        private final int[][] positions = new int[PARTITION_COUNT][POSITIONS_PER_PAGE];
        private final int[] positionCounts = new int[PARTITION_COUNT];

        private int[] intValues;
        private byte[] intBuffer;
        private long[][][] longValues; // = new long[PAGE_COUNT][CHANNEL_COUNT][POSITIONS_PER_PAGE];
        private Object shortValues;
        private boolean[][][] nulls;
        private byte[][][] longValuesBuffers; // = new byte[PARTITION_COUNT][CHANNEL_COUNT][PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        private Object shortValuesBuffers;
        private SliceOutput[][] sliceOutput;
        private OptimizedSliceOutput[][] optimizedSliceOutput;
        private Page[] bigintDataPages; // = new Page[PAGE_COUNT];
        private Page[] shortDataPages; // = new Page[PAGE_COUNT];
        private Page[] mixedTypeDataPages; // = new Page[PAGE_COUNT];
        private int[][] longValueBufferIndexes;
        private int[][] shortValueBufferIndexes;

        @Param({"1", "2", "4"})
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean mayHaveNull;

        @Setup
        public void setup()
        {
            createNulls();
            createBigintPages();
            createBigintValues();

            createBigintByteArrayBuffers();
            createBigintByteArrayBufferIndex();

            createSliceOutput();

            createOptimizedSliceOutput();
//
//            createShortValues();
//            createShortPages();
//            createShortByteArrayBuffers();
//            createShortByteArrayBufferIndex();

//            createMixedTypesPages();
            populatePositions();

            createIntValues();
        }

//        public boolean mayHaveNull()
//        {
//            return mayHaveNull;
//        }

        public Page getBigintDataPage(int pageIndex)
        {
            return bigintDataPages[pageIndex];
        }

        public Page getMixedTypeDataPage(int pageIndex)
        {
            return mixedTypeDataPages[pageIndex];
        }

        public boolean[] getNulls(int pageIndex, int channelIndex)
        {
            return ((boolean[][][]) nulls)[pageIndex][channelIndex];
        }

        public long[] getLongValues(int pageIndex, int channelIndex)
        {
            return ((long[][][]) longValues)[pageIndex][channelIndex];
        }

        public short[] getShortValues(int pageIndex, int channelIndex)
        {
            return ((short[][][]) shortValues)[pageIndex][channelIndex];
        }

        public int[] getPositions(int partition)
        {
            return positions[partition];
        }

        public int getPostistionCount(int partition)
        {
            return positionCounts[partition];
        }

        public byte[] getLongValueBuffer(int partition, int channelIndex)
        {
            return ((byte[][][]) longValuesBuffers)[partition][channelIndex];
        }

        public byte[] getShortValueBuffer(int partition, int channelIndex)
        {
            return ((byte[][][]) shortValuesBuffers)[partition][channelIndex];
        }

        public byte[] getLongValueDynamicSliceOutput(int partition, int channelIndex)
        {
            return ((byte[][][]) longValuesBuffers)[partition][channelIndex];
        }

        public byte[] getLongValueBasicSliceOutput(int partition, int channelIndex)
        {
            return ((byte[][][]) longValuesBuffers)[partition][channelIndex];
        }

        public int[][] getLongValueBufferIndexes()
        {
            for (int j = 0; j < PARTITION_COUNT; j++) {
                Arrays.fill(longValueBufferIndexes[j], ARRAY_BYTE_BASE_OFFSET);
            }
            return longValueBufferIndexes;
        }

        public int[][] getShortValueBufferIndexes()
        {
            for (int j = 0; j < PARTITION_COUNT; j++) {
                Arrays.fill(shortValueBufferIndexes[j], ARRAY_BYTE_BASE_OFFSET);
            }
            return shortValueBufferIndexes;
        }

        public void resetSliceOutput()
        {
            for (int pageIndex = 0; pageIndex < PARTITION_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    sliceOutput[pageIndex][channelIndex].reset();
                }
            }
        }

        public void resetOptimizedSliceOutput()
        {
            for (int pageIndex = 0; pageIndex < PARTITION_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    optimizedSliceOutput[pageIndex][channelIndex].reset();
                }
            }
        }

        public int getPageCount()
        {
            return PAGE_COUNT;
        }

        private void createBigintPages()
        {
            bigintDataPages = new Page[PAGE_COUNT];
            //types = ImmutableList.of(BIGINT, SMALLINT);
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount];
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    if (mayHaveNull) {
                        blocks[channelIndex] = createBigintWithNullsChannel();
                    }
                    else {
                        blocks[channelIndex] = createBigintChannel();
                    }
                }
                bigintDataPages[pageIndex] = new Page(blocks);
            }
        }

        private void createBigintWithNullsPages()
        {
            bigintDataPages = new Page[PAGE_COUNT];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount];
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    blocks[channelIndex] = createBigintChannel();
                }
                bigintDataPages[pageIndex] = new Page(blocks);
            }
        }

        private void createShortPages()
        {
            shortDataPages = new Page[PAGE_COUNT];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount];
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    blocks[channelIndex] = createShortChannel();
                }
                shortDataPages[pageIndex] = new Page(blocks);
            }
        }

        private void createMixedTypesPages()
        {
            mixedTypeDataPages = new Page[PAGE_COUNT];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount * 2];

                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    blocks[channelIndex] = createBigintChannel();
                }

                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    blocks[channelCount + channelIndex] = createShortChannel();
                }

                mixedTypeDataPages[pageIndex] = new Page(blocks);
            }
        }

        private Block createBigintChannel()
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                BIGINT.writeLong(blockBuilder, position);
            }
            return blockBuilder.build();
        }

        private Block createBigintWithNullsChannel()
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    BIGINT.writeLong(blockBuilder, position);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            return blockBuilder.build();
        }

        private Block createShortChannel()
        {
            BlockBuilder blockBuilder = SMALLINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                SMALLINT.writeLong(blockBuilder, position);
            }
            return blockBuilder.build();
        }

        private void createIntValues()
        {
            intValues = new int[PAGE_COUNT * POSITIONS_PER_PAGE];
            intBuffer = new byte[PAGE_COUNT * POSITIONS_PER_PAGE * 4];
            for (int position = 0; position < PAGE_COUNT * POSITIONS_PER_PAGE; position++) {
                intValues[position] = position;
            }
        }

        private void createBigintValues()
        {
            longValues = new long[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        ((long[][][]) longValues)[pageIndex][channelIndex][position] = (long) position;
                    }
                }
            }
        }

        private void createShortValues()
        {
            shortValues = new short[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        ((short[][][]) shortValues)[pageIndex][channelIndex][position] = (short) position;
                    }
                }
            }
        }

        private void createNulls()
        {
            nulls = new boolean[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        nulls[pageIndex][channelIndex][position] = ThreadLocalRandom.current().nextBoolean();
                    }
                }
            }
        }

        private void createBigintByteArrayBuffers()
        {
            longValuesBuffers = new byte[PARTITION_COUNT][channelCount][PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        }

        private void createShortByteArrayBuffers()
        {
            shortValuesBuffers = new byte[PARTITION_COUNT][channelCount][PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_SHORT_INDEX_SCALE];
        }

        private void createBigintByteArrayBufferIndex()
        {
            longValueBufferIndexes = new int[PARTITION_COUNT][channelCount];
        }

        private void createShortByteArrayBufferIndex()
        {
            shortValueBufferIndexes = new int[PARTITION_COUNT][channelCount];
        }

        private void createSliceOutput()
        {
            sliceOutput = new BasicSliceOutput[PARTITION_COUNT][channelCount];
            for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    Slice slice = Slices.wrappedBuffer(new byte[PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE]);
                    //sliceOutput[pageIndex][channelIndex] = new DynamicSliceOutput(PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);
                    sliceOutput[partitionIndex][channelIndex] = slice.getOutput();
                }
            }
        }

        private void createOptimizedSliceOutput()
        {
            optimizedSliceOutput = new OptimizedSliceOutput[PARTITION_COUNT][channelCount];
            for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    optimizedSliceOutput[partitionIndex][channelIndex] = new OptimizedSliceOutput(10*PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);
                }
            }
        }

        private void populatePositions()
        {
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                int partition = ThreadLocalRandom.current().nextInt(PARTITION_COUNT);
                int indexForPartition = positionCounts[partition];
                positions[partition][indexForPartition] = i;
                positionCounts[partition]++;
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        //new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongValuesToOptimizedSliceOutput(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongValuesToByteArrayBuffer2(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongPagesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongPagesToByteArrayBuffer2(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addCompositeValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addCompositePagesToByteArrayBuffer(data);
       // new BenchmarkPartitionedOutputOperatorAppendToBuffers().addLongPagesToDynamicSliceOutput(data);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx10g") //,
//                        "-XX:+UnlockDiagnosticVMOptions",
//                        "-XX:+PrintAssembly",
//                        "-XX:PrintAssemblyOptions=intel",
//                        "-XX:CompileCommand=print,*BenchmarkPartitionedOutputOperatorAppendToBuffers.addLongValuesToOptimizedSliceOutput")

                .include(".*" + BenchmarkPartitionedOutputOperatorAppendToBuffers.class.getSimpleName() + ".*")
                //.addProfiler(LinuxPerfAsmProfiler.class, "events=cpu-clock")
                .build();
        new Runner(options).run();
    }
}
