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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.orc.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.packBytes;
import static com.facebook.presto.orc.reader.ReaderUtils.packBytesAndNulls;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackByteNulls;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BooleanSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanSelectiveStreamReader.class).instanceSize();
    private static final Block NULL_BLOCK = BOOLEAN.createBlockBuilder(null, 1).appendNull().build();

    private final StreamDescriptor streamDescriptor;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<BooleanInputStream> dataStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;
    @Nullable
    private byte[] values;
    @Nullable
    private boolean[] nulls;
    @Nullable
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private boolean valuesInUse;

    private OrcLocalMemoryContext systemMemoryContext;

    public BooleanSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            OrcLocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, Map<Integer, ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, BooleanInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;
//        if (outputRequired) {
//            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
//        }

        if (useBatchMode()) {
            // values need to be totalPositionCount,
            // and nulls need to be allocated even nullsAllowed == false, and whether there is filter or not, or outputRequired == true or not,
            // because values need to be unpacked with nulls
            ensureValuesCapacity(positions[positionCount - 1] + 1, presentStream != null);
        }
        else if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (filter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            if (useBatchMode()) {
                // WHen there is no nulls, this is faster, But when there is partial nulls, this is slower
                int totalPositionCount = positions[positionCount - 1] + 1;
                int readCount = 0;

                final int filteredPositionCount;

                if (presentStream == null) {
                    dataStream.getSetBits(totalPositionCount, values);

                    filteredPositionCount = evaluateFilter(positions, positionCount);

                    if (outputRequired && totalPositionCount > filteredPositionCount) {
                        packBytes(values, outputPositions, filteredPositionCount);
                    }
                }
                else {
                    int nullCount = presentStream.getUnsetBits(totalPositionCount, nulls);

                    if (nullCount == totalPositionCount) {
                        // all nulls
                        allNulls = true;
                        filteredPositionCount = positionCount; // No positions were filtered out
                    }
                    else {
                        // some nulls
                        readCount = totalPositionCount - nullCount;
                        dataStream.getSetBits(readCount, values);

                        if (nullCount != 0) {
                            // Note it should be totalPositionCount instead of positionCound
                            unpackByteNulls(values, nulls, totalPositionCount, readCount);
                        }

                        filteredPositionCount = evaluateFilterWithNulls(positions, positionCount);

                        if (outputRequired && totalPositionCount > filteredPositionCount) {
                            // both values and nulls need to be packed
                            packBytesAndNulls(values, nulls, outputPositions, filteredPositionCount);
                        }
                    }
                }
                outputPositionCount = filteredPositionCount;

                // For this reader, should return filteredPositionCount instead of totalPositionCount
                readOffset = offset + totalPositionCount;
                return filteredPositionCount;
            }

            // This branch is inlined because extracting this code into a helper method (readWithFilter)
            // results in performance regressions on BenchmarkSelectiveStreamReaders.readBooleanNoNull[withFilter]
            // benchmarks.
            // The differences are:
            // - readBooleanNoNull: 0.045 vs.0.029 s/op
            // - readBooleanNoNullWithFilter: 0.112 vs 0.099 s/op
            outputPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    skip(position - streamPosition);
                    streamPosition = position;
                }

                if (presentStream != null && !presentStream.nextBit()) {
                    if ((nonDeterministicFilter && filter.testNull()) || nullsAllowed) {
                        if (outputRequired) {
                            nulls[outputPositionCount] = true;
                        }
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount++;
                    }
                }
                else {
                    boolean value = dataStream.nextBit();
                    if (filter.testBoolean(value)) {
                        if (outputRequired) {
                            values[outputPositionCount] = (byte) (value ? 1 : 0);
                            if (nullsAllowed && presentStream != null) {
                                nulls[outputPositionCount] = false;
                            }
                        }
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount++;
                    }
                }

                outputPositionCount -= filter.getPrecedingPositionsToFail();

                streamPosition++;

                int succeedingPositionsToFail = filter.getSucceedingPositionsToFail();
                if (succeedingPositionsToFail > 0) {
                    int positionsToSkip = 0;
                    for (int j = 0; j < succeedingPositionsToFail; j++) {
                        i++;
                        int nextPosition = positions[i];
                        positionsToSkip += 1 + nextPosition - streamPosition;
                        streamPosition = nextPosition + 1;
                    }
                    skip(positionsToSkip);
                }
            }
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nonDeterministicFilter) {
            outputPositionCount = 0;
            for (int i = 0; i < positionCount; i++) {
                if (filter.testNull()) {
                    outputPositionCount++;
                }
                else {
                    outputPositionCount -= filter.getPrecedingPositionsToFail();
                    i += filter.getSucceedingPositionsToFail();
                }
            }
        }
        else if (nullsAllowed) {
            outputPositionCount = positionCount;
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        // filter == null implies outputRequired == true
        if (presentStream == null && positions[positionCount - 1] == positionCount - 1) {
            // contiguous chunk of rows, no nulls
            dataStream.getSetBits(positionCount, values);
            outputPositionCount = positionCount;
            return positionCount;
        }

        if (useBatchMode()) {
            int totalPositionCount = positions[positionCount - 1] + 1;
            if (presentStream == null) {
                dataStream.getSetBits(totalPositionCount, values);

                if (totalPositionCount > positionCount) {
                    packBytes(values, positions, positionCount);
                }
            }
            else {
                int nullCount = presentStream.getUnsetBits(totalPositionCount, nulls);

                if (nullCount == totalPositionCount) {
                    // all nulls
                    allNulls = true;
                }
                else {
                    // some nulls
                    dataStream.getSetBits(totalPositionCount - nullCount, values);

                    if (outputRequired) {
                        if (nullCount != 0) {
                            unpackByteNulls(values, nulls, totalPositionCount, totalPositionCount - nullCount);
                        }

                        if (totalPositionCount > positionCount) {
                            // Need to pack both values and nulls
                            packBytesAndNulls(values, nulls, positions, positionCount);
                        }
                    }
                }
            }
            outputPositionCount = positionCount;
            return totalPositionCount;
        }

        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
            }
            else {
                values[i] = (byte) (dataStream.nextBit() ? 1 : 0);
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
        }
    }

    private void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        values = ensureCapacity(values, capacity);

        if (recordNulls) {
            nulls = ensureCapacity(nulls, capacity);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(NULL_BLOCK, positionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = new ByteArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
            values = null;
            return block;
        }

        byte[] valuesCopy = new byte[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            valuesCopy[positionIndex] = this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new ByteArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(NULL_BLOCK, positionCount));
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        return newLease(new ByteArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values));
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
    }

    private BlockLease newLease(Block block)
    {
        valuesInUse = true;
        return ClosingBlockLease.newLease(block, () -> valuesInUse = false);
    }

    private void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            assert outputPositions[i] == nextPosition;

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        nulls = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;

        systemMemoryContext.close();
    }

    private boolean useBatchMode()
    {
        return true;
    }

    private int evaluateFilter(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        int i = 0;
        while (i < positionCount) {
            int position = positions[i];
            if (filter.testBoolean(values[position])) {
                outputPositions[positionsIndex++] = position;  // compact positions on the fly
                i++;
            }
            else {
                i += filter.getSucceedingPositionsToFail() + 1;
                positionsIndex -= filter.getPrecedingPositionsToFail();
            }
        }
        return positionsIndex;
    }

    private int evaluateFilterWithNulls(int[] positions, int positionCount)
    {
        boolean testNull = (nonDeterministicFilter && filter.testNull()) || nullsAllowed;

        int positionsIndex = 0;
        int i = 0;
        while (i < positionCount) {
            int position = positions[i];

            // Note it should not be nulls[position] && testNull
            if (nulls[position]) {
                if (testNull) {
                    outputPositions[positionsIndex++] = position;
                }
            }
            else {
                if (filter.testBoolean(values[position])) {
                    outputPositions[positionsIndex++] = position;  // compact positions on the fly
                }
                else {
                    i += filter.getSucceedingPositionsToFail();
                    positionsIndex -= filter.getPrecedingPositionsToFail();
                }
            }
            i++;
        }
        return positionsIndex;
    }
}
