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
import com.facebook.presto.common.block.ClosingBlockLease;
import com.facebook.presto.common.block.IntArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.FloatInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.orc.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.packInts;
import static com.facebook.presto.orc.reader.ReaderUtils.packIntsAndNulls;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackIntsWithNulls;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;

public class FloatSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FloatSelectiveStreamReader.class).instanceSize();
    private static final Block NULL_BLOCK = REAL.createBlockBuilder(null, 1).appendNull().build();

    private final StreamDescriptor streamDescriptor;
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;
    private final OrcLocalMemoryContext systemMemoryContext;
    private final boolean nonDeterministicFilter;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<FloatInputStream> dataStreamSource = missingStreamSource(FloatInputStream.class);
    private BooleanInputStream presentStream;
    private FloatInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;
    private int[] values;
    private boolean[] nulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private boolean valuesInUse;

    public FloatSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            OrcLocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if outputRequired is false");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        this.nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, Map<Integer, ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, FloatInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
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
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode()) {
            // values need to be allocated for batch mode, because they need to be used for evaluating filters even when output is not required,
            // nulls need to be allocated when presentStream != null, because values need to be unpacked with nulls
            ensureValuesCapacity(totalPositionCount, presentStream != null);
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
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        boolean testNull = (nonDeterministicFilter && this.filter.testNull()) || nullsAllowed;

        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode()) {
            int readCount = 0;

            final int filteredPositionCount;

            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);

                filteredPositionCount = evaluateFilter(positions, positionCount);

                if (outputRequired && totalPositionCount > filteredPositionCount) {
                    packInts(values, outputPositions, filteredPositionCount);
                }
            }
            else {
                int nullCount = presentStream.getUnsetBits(totalPositionCount, nulls);
                if (nullCount == totalPositionCount) {
                    // all nulls
                    if (testNull) {
                        allNulls = true;
                        filteredPositionCount = positionCount; // No positions were filtered out
                    }
                    else {
                        filteredPositionCount = 0;
                    }
                }
                else {
                    // some nulls
                    readCount = totalPositionCount - nullCount;
                    dataStream.next(values, readCount);

                    if (nullCount != 0) {
                        // Note it should be totalPositionCount instead of positionCound
                        unpackIntsWithNulls(values, nulls, totalPositionCount, readCount);
                    }

                    filteredPositionCount = evaluateFilterWithNulls(positions, positionCount, testNull);

                    if (outputRequired && totalPositionCount > filteredPositionCount) {
                        // both values and nulls need to be packed
                        packIntsAndNulls(values, nulls, outputPositions, filteredPositionCount);
                    }
                }
            }

            outputPositionCount = filteredPositionCount;

            // Should return totalPositionCount instead of readCount
            return totalPositionCount;
        }

        int streamPosition = 0;
        outputPositionCount = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (testNull) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                float value = dataStream.next();
                if (filter.testFloat(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = floatToRawIntBits(value);
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPosition++;

            if (filter != null) {
                outputPositionCount -= filter.getPrecedingPositionsToFail();
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

        return streamPosition;
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
        int totalPositionCount = positions[positionCount - 1] + 1;
        if (useBatchMode()) {
            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);
                if (totalPositionCount > positionCount) {
                    packInts(values, positions, positionCount);
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
                    dataStream.next(values, totalPositionCount - nullCount);

                    if (outputRequired) {
                        if (nullCount != 0) {
                            unpackIntsWithNulls(values, nulls, totalPositionCount, totalPositionCount - nullCount);
                        }

                        if (totalPositionCount > positionCount) {
                            // Need to pack both values and nulls
                            packIntsAndNulls(values, nulls, positions, positionCount);
                        }
                    }
                }
            }
            outputPositionCount = positionCount;
            return totalPositionCount;
        }

        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream != null) {
                // some nulls
                int nullCount = presentStream.getUnsetBits(positionCount, nulls);
                if (nullCount == positionCount) {
                    allNulls = true;
                }
                else {
                    for (int i = 0; i < positionCount; i++) {
                        if (!nulls[i]) {
                            values[i] = floatToRawIntBits(dataStream.next());
                        }
                    }
                }
            }
            else {
                // no nulls
                for (int i = 0; i < positionCount; i++) {
                    values[i] = floatToRawIntBits(dataStream.next());
                }
            }
            outputPositionCount = positionCount;
            return positions[positionCount - 1] + 1;
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
                values[i] = floatToRawIntBits(dataStream.next());
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
            Block block = new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
            values = null;
            return block;
        }

        int[] valuesCopy = new int[positionCount];
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

        return new IntArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
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

        return newLease(new IntArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values));
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
    public void throwAnyError(int[] positions, int positionCount)
    {
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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private boolean useBatchMode()
    {
        // JMH benchmark shows that when there is no null or no filter, the batch read mode was always better than the skipping mode.
        // When there is filter and partial nulls, the batch read mode was better than the skipping mode when the input filter rate is less than 0.4
        if (presentStream == null || filter == null) {
            return true;
        }

        return false;
    }

    private int evaluateFilter(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        int i = 0;
        while (i < positionCount) {
            int position = positions[i];
            if (filter.testFloat(intBitsToFloat(values[position]))) {
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

    private int evaluateFilterWithNulls(int[] positions, int positionCount, boolean testNull)
    {
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
                if (filter.testFloat(intBitsToFloat(values[position]))) {
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
