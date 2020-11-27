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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.ClosingBlockLease.newLease;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.ReaderUtils.packLongs;
import static com.facebook.presto.orc.reader.ReaderUtils.packLongsAndNulls;
import static com.facebook.presto.orc.reader.ReaderUtils.unpackLongsWithNulls;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LongDirectSelectiveStreamReader
        extends AbstractLongSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDirectSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nonDeterministicFilter;
    private final boolean nullsAllowed;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;

    private boolean allNulls;

    private OrcLocalMemoryContext systemMemoryContext;

    public LongDirectSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            OrcLocalMemoryContext systemMemoryContext)
    {
        super(outputType);
        requireNonNull(filter, "filter is null");
        checkArgument(filter.isPresent() || outputRequired, "filter must be present if output is not required");
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nonDeterministicFilter = this.filter != null && !this.filter.isDeterministic();
        nullsAllowed = this.filter == null || nonDeterministicFilter || this.filter.testNull();
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (useBatchMode()) {
            // values need to be totalPositionCount,
            // and nulls need to be allocated even nullsAllowed == false, and whether there is filter or not, or outputRequired == true or not,
            // because values need to be unpacked with nulls
            prepareNextRead(positions[positionCount - 1] + 1, presentStream != null, true);
        }
        else {
            prepareNextRead(positionCount, nullsAllowed && presentStream != null);
        }

        allNulls = false;

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition;
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
        if (useBatchMode()) {
            int totalPositionCount = positions[positionCount - 1] + 1;
            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);
                if (totalPositionCount > positionCount) {
                    packLongs(values, positions, positionCount);
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
                            unpackLongsWithNulls(values, nulls, totalPositionCount, totalPositionCount - nullCount);
                        }

                        if (totalPositionCount > positionCount) {
                            // Need to pack both values and nulls
                            packLongsAndNulls(values, nulls, positions, positionCount);
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
                        if (nulls[i]) {
                            values[i] = 0;
                        }
                        else {
                            values[i] = dataStream.next();
                        }
                    }
                }
            }
            else {
                // no nulls
                for (int i = 0; i < positionCount; i++) {
                    values[i] = dataStream.next();
                }
            }
            outputPositionCount = positionCount;
            return positionCount;
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
                values[i] = 0;
            }
            else {
                values[i] = dataStream.next();
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }

            streamPosition++;
        }

        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        if (useBatchMode()) {
            int totalPositionCount = positions[positionCount - 1] + 1;
            int readCount = 0;
            final int filteredPositionCount;

            if (presentStream == null) {
                dataStream.next(values, totalPositionCount);

                filteredPositionCount = evaluateFilter(positions, positionCount);

                if (outputRequired && totalPositionCount > filteredPositionCount) {
                    packLongs(values, outputPositions, filteredPositionCount);
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
                    dataStream.next(values, readCount);

                    if (nullCount != 0) {
                        // Note it should be totalPositionCount insted of positionCound
                        unpackLongsWithNulls(values, nulls, totalPositionCount, readCount);
                    }

                    filteredPositionCount = evaluateFilterWithNulls(positions, positionCount);

                    if (outputRequired && totalPositionCount > filteredPositionCount) {
                        // both values and nulls need to be packed
                        packLongsAndNulls(values, nulls, outputPositions, filteredPositionCount);
                    }
                }
            }
            outputPositionCount = filteredPositionCount;

            // Should return totalPositionCount instead of readCount
            return totalPositionCount;
        }

        if (positions[positionCount - 1] == positionCount - 1) {
            // no skipping
            if (presentStream == null) {
                // no nulls
                if (!outputRequired && !filter.isPositionalFilter()) {
                    // no output; just filter
                    for (int i = 0; i < positionCount; i++) {
                        long value = dataStream.next();
                        if (filter.testLong(value)) {
                            outputPositions[outputPositionCount] = positions[i];
                            outputPositionCount++;
                        }
                    }
                    return positionCount;
                }
            }
        }

        int streamPosition = 0;

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
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                long value = dataStream.next();
                if (filter.testLong(value)) {
                    if (outputRequired) {
                        values[outputPositionCount] = value;
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }

            streamPosition++;

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

//            if (succeedingPositionsToFail > 0) {
//                int positionsToSkip = positions[i + succeedingPositionsToFail] - positions[i];
//                skip(positionsToSkip);
//                streamPosition += positionsToSkip;
//                i += succeedingPositionsToFail;
//            }
        }

        return streamPosition;
    }
//
//    private void printNulls(boolean[] nulls, int positionCount)
//    {
//        String str = "";
//        for (int i = 0; i < positionCount; i++) {
//            if (nulls[i]) {
//                str += i + " ";
//            }
//        }
//        System.out.println(str);
//    }
//
//    private void printValues(long[] values, int positionCount)
//    {
//        String str = "";
//        for (int i = 0; i < positionCount; i++) {
//            str += i + ":" + values[i] + " ";
//        }
//        System.out.println(str);
//    }
//
//    private void printPos(int[] values, int positionCount)
//    {
//        String str = "";
//        for (int i = 0; i < positionCount; i++) {
//            str += values[i] + " ";
//        }
//        System.out.println(str);
//    }

    protected boolean useBatchMode()
    {
        return true;
    }

//    private int evaluateFilter(int[] positions, int positionCount)
//    {
//        int positionsIndex = 0;
//        for (int i = 0; i < positionCount; i++) {
//            int position = positions[i];
//            System.out.println("i=" + i + " position=" + position);
//            if (filter.testLong(values[position])) {
//                outputPositions[positionsIndex++] = position;  // compact positions on the fly
//            }
//        }
//        return positionsIndex;
//    }

//    private int evaluateFilterWithNulls(int[] positions, int positionCount)
//    {
//        boolean testNull = (nonDeterministicFilter && filter.testNull()) || nullsAllowed;
//
//        int positionsIndex = 0;
//        for (int i = 0; i < positionCount; i++) {
//            int position = positions[i];
//
//            // Note it should not be nulls[position] && testNull
//            if (nulls[position]) {
//                if (testNull) {
//                    outputPositions[positionsIndex++] = position;
//                }
//            }
//            else {
//                if (filter.testLong(values[position])) {
//                    outputPositions[positionsIndex++] = position;  // compact positions on the fly
//                }
//            }
//        }
//        return positionsIndex;
//    }

    private int evaluateFilter(int[] positions, int positionCount)
    {
        int positionsIndex = 0;
        int i = 0;
        while (i < positionCount) {
            int position = positions[i];
//            System.out.println("i=" + i + " position=" + position);
            if (filter.testLong(values[position])) {
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
                if (filter.testLong(values[position])) {
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

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount);
        }

        return buildOutputBlock(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return newLease(new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), positionCount));
        }

        return buildOutputBlockView(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, Map<Integer, ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;

        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + super.getRetainedSizeInBytes();
    }

//    protected boolean useBatchMode(int positionCount, int totalPositionCount)
//    {
//        return false;
//        // maxCodePointCount < 0 means it's unbounded varchar VARCHAR.
//        // If the types are VARCHAR(N) or CHAR(N), the length of the string need to be calculated and truncated.
//
//    }
}
