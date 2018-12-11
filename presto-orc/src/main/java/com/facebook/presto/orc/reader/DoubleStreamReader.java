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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import  com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.DoubleInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Double.doubleToLongBits;
import static java.lang.Double.longBitsToDouble;
import static java.util.Objects.requireNonNull;

public class DoubleStreamReader
    extends ColumnReader
    implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DoubleStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    private boolean[] nullVector = new boolean[0];
    private boolean[] valueIsNull;
    private long[] values;
    // Number of positions in values, valueIsNull.
    private int numValues = 0;
    // Number of result rows in scan() so far.
    private int numResults;
    //Present flag for each row in input QualifyingSet.
    boolean[] present;
    // position of first element of lengths from the start of RowGroup.
    int posInRowGroup;
    // positions of non-null values from start of row group. Set only if presentStream != null.
    int[] nonNullRows;
    // Result arrays from outputQualifyingSet.
    int[] outputRows;
    int[] resultInputNumbers;

    private InputStreamSource<DoubleInputStream> dataStreamSource = missingStreamSource(DoubleInputStream.class);
    @Nullable
    private DoubleInputStream dataStream;

    private boolean rowGroupOpen;

    private LocalMemoryContext systemMemoryContext;

    public DoubleStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                dataStream.skip(readOffset);
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);
        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            dataStream.nextVector(type, nextBatchSize, builder);
        }
        else {
            for (int i = 0; i < nextBatchSize; i++) {
                if (presentStream.nextBit()) {
                    verify(dataStream != null);
                    type.writeDouble(builder, dataStream.next());
                }
                else {
                    builder.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return builder.build();
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        posInRowGroup = 0;
        rowGroupOpen = true;
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(DoubleInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, DoubleInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

        @Override
    public int erase(int begin, int end, int numResultsBeforeRowGroup, int numErasedFromInput)
    {
        if (block != null) {
            block.erase(numResultsBeforeRowGroup + begin, block.getPositionCount());
        }
        if (valueIsNull != null) {
            Arrays.fill(valueIsNull, false);
        }
        numValues = 0;
        return 0;
    }
    
    @Override
    public int scan(int maxBytes)
            throws IOException
    {
        if (filter != null && outputQualifyingSet == null) {
            outputQualifyingSet = new QualifyingSet();
        }
        if (!rowGroupOpen) {
            openRowGroup();
        }
        numResults = 0;
        int numNonNull = 0;
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        int numInput = input.getPositionCount();
        int end = input.getEnd();
        int rowsInRange = end - posInRowGroup;
        int valuesSize = end;
        if (presentStream != null) {
            if (nonNullRows == null || nonNullRows.length < end) {
                nonNullRows = new int[end];
            }
            if (present == null || present.length < end) {
                present = new boolean[end];
            }
            presentStream.getSetBits(rowsInRange, present);
            for (int i = 0; i < rowsInRange; i++) {
                if (present[i]) {
                    numNonNull++;
                    if (filter != null) {
                                                nonNullRows[numNonNull - 1] = i + posInRowGroup;
                    }
                }
            }
        }
        if (values == null || values.length < valuesSize) {
            values = new long[valuesSize];
        }
        outputRows = filter != null ? output.getMutablePositions(rowsInRange) : null;
        resultInputNumbers = filter != null ? output.getMutableInputNumbers(rowsInRange) : null;
        int[] inputPositions = input.getPositions();
        int valueIdx = 0;
        int nextActive = inputPositions[0];
        int activeIdx = 0;
        int numActive = input.getPositionCount();
        int toSkip = 0;
        for (int i = 0; i < rowsInRange; i++) {
            if (i + posInRowGroup == nextActive) {
                if (present != null && !present[i]) {
                    if (filter == null || filter.testNull()) {
                        addNullResult(i + posInRowGroup, activeIdx);
                    }
                }
                    else {
                        // Non-null row in qualifying set.
                        if (toSkip > 0) {
                        dataStream.skip(toSkip);
                        toSkip = 0;
                    }
                    if (filter != null) {
                        double value = dataStream.next();
                        if (filter.testDouble(value)) {
                            outputRows[numResults] = presentStream != null ? nonNullRows[valueIdx] : i + posInRowGroup;
                            resultInputNumbers[numResults] = activeIdx;
                            if (outputChannel != -1) {
                                addResult(value);
                            }
                            numResults++;
                        }
                    }
                    else {
                        // No filter.
                        addResult(dataStream.next());
                        numResults++;
                    }
                    valueIdx++;
                    }
                if (++activeIdx == numActive) {
                    toSkip = numNonNull - valueIdx;
                    break;
                }
                nextActive = inputPositions[activeIdx];
                continue;
            }
            else {
                // The row is notg in the input qualifying set. Add to skip if non-null.
                if (present == null || present[i]) {
                    toSkip ++;
                    valueIdx++;
                }
            }
        }   
        if (toSkip > 0) {
            dataStream.skip(toSkip);
        }
        if (output != null) {
            output.setPositionCount(numResults);
        }
        numValues += numResults;
        if (block != null) {
            block.setPositionCount(numValues);
        }
        if (output != null) {
            output.setEnd(end);
        }
        posInRowGroup = end;
        return end;
    }

    void addNullResult(int row, int activeIdx)
    {
        if (outputChannel != -1) {
            if (valueIsNull == null) {
                valueIsNull = new boolean[values.length];
            }
            ensureResultRows();
            valueIsNull[numResults + numValues] = true;
        }
        if (filter != null) {
            outputRows[numResults] = row;
            resultInputNumbers[numResults] = activeIdx;
        }
        numResults++;
    }

    void addResult(double value)
    {
        ensureResultRows();
        values[numValues + numResults] = doubleToLongBits(value);
        if (valueIsNull != null) {
            valueIsNull[numValues + numResults] = false;
        }
    }

    void ensureResultRows()
    {
        if (values.length <= numValues + numResults) {
            values = Arrays.copyOf(values, Math.max(numValues + numResults + 10, values.length * 2));
            block = null;
        }
        if (valueIsNull != null) {
                valueIsNull = Arrays.copyOf(valueIsNull, values.length);
                block = null;
        }
    }
   
    @Override
    public Block getBlock(boolean mayReuse)
    {
        if (block == null) {
            if (values == null) {
                values = new long[numValues];
            }
            block = new LongArrayBlock(numValues, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
        }
        Block oldBlock = block;
        if (!mayReuse) {
            values = null;
            valueIsNull = null;
            block = null;
            numValues = 0;
        }
        return oldBlock;
    }

    
    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
        nullVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector);
    }
}
