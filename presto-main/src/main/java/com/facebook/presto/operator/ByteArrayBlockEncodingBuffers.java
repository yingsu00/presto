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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.operator.UncheckedByteArrays.setByte;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;

public class ByteArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = Byte.BYTES + Byte.BYTES;

    private static final String NAME = "BYTE_ARRAY";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteArrayBlockEncodingBuffers.class).instanceSize();

    private byte[] valuesBuffer;
    private int valuesBufferIndex;

    public ByteArrayBlockEncodingBuffers(int initialPositionCount)
    {
        super(initialPositionCount);
        prepareBuffers();
    }

    @Override
    protected void prepareBuffers()
    {
        valuesBuffer = new byte[initialPositionCount];
        valuesBufferIndex = 0;
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        valuesBufferIndex = 0;
        resetNullsBuffer();
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        throw new UnsupportedOperationException("accumulateRowSizes is not supported for fixed width types");
    }

    @Override
    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += (positionOffsets[i + 1] - positionOffsets[i]) * POSITION_SIZE;
        }
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendValuesToBuffer();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    public void serializeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);

        sliceOutput.writeInt(bufferedPositionCount);

        serializeNullsTo(sliceOutput);

        sliceOutput.appendBytes(valuesBuffer, 0, valuesBufferIndex);
    }

    @Override
    public long getSizeInBytes()
    {
        return getPositionsSizeInBytes() + // positions and mappedPositions
                valuesBufferIndex +
                getNullsBufferSizeInBytes();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPostionsRetainedSizeInBytes() +
                sizeOf(valuesBuffer) +
                getNullsBufferRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                valuesBufferIndex +             // values buffer
                getNullsBufferSerializedSizeInBytes();    // nulls buffer
    }

    private void appendValuesToBuffer()
    {
        ensureValueBufferCapacity();

        int[] positions = getPositions();
        if (decodedBlock.mayHaveNull()) {
            for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
                int position = positions[i];
                byte value = decodedBlock.getByte(position);
                int newIndex = setByte(valuesBuffer, valuesBufferIndex, value);

                if (!decodedBlock.isNull(position)) {
                    valuesBufferIndex = newIndex;
                }
            }
        }
        else {
            for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
                byte value = decodedBlock.getByte(positions[i]);
                valuesBufferIndex = setByte(valuesBuffer, valuesBufferIndex, value);
            }
        }
    }

    private void ensureValueBufferCapacity()
    {
        int capacity = valuesBufferIndex + batchSize;
        if (valuesBuffer.length < capacity) {
            valuesBuffer = Arrays.copyOf(valuesBuffer, max(valuesBuffer.length * 2, capacity));
        }
    }
}
