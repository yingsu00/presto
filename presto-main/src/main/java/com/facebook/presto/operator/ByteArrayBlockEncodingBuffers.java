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

import com.facebook.presto.spi.block.Block;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class ByteArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "BYTE_ARRAY";
    public static final int DEFAULT_MAX_ELEMENT_COUNT = 1024 * 1024;

    // These are single piece buffers for now. They will be requested from buffer pool dynamically
//    private byte[] nullsBuffer;
//    private int nullsBufferIndex;
    private byte[] valuesBuffer;
    private int valuesBufferIndex;

    ByteArrayBlockEncodingBuffers(int[] positions)
    {
        this.positions = positions;
        prepareBuffers();
    }

    @Override
    void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future.
//        if (nullsBuffer == null) {
//            nullsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT / ARRAY_BYTE_INDEX_SCALE];
//        }
//        nullsBufferIndex = 0;

        if (valuesBuffer == null) {
            valuesBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT];
        }
        valuesBufferIndex = 0;
    }

    void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        containsNull = false;
        valuesBufferIndex = 0;
    }

    void setColumnarObject(PartitionedOutputOperator.ColumnarObjectNode columnarObjectNode)
    {
        throw new UnsupportedOperationException("LongArrayBlockEncodingBuffers does not support setColumnarObject");
    }

    @Override
    void copyValues(Block block)
    {
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 && positions[positionsOffset + batchSize - 1] < block.getPositionCount());
//                format("positionOffset %d + batchSize %d - 1 should be less than the positions array size %d, " +
//                                "and the start position %d in the positions array should be greater than or equal to 0, " +
//                                "and the largest position %d in the positions array should be smaller than block positionCount %d.",
//                        positionsOffset,
//                        batchSize,
//                        positions.length,
//                        positions[positionsOffset],
//                        positions[positionsOffset + batchSize - 1],
//                        block.getPositionCount()));

        appendValuesToBuffer(block);
        appendNulls(block);
        bufferedPositionCount += batchSize;
    }

    void writeTo(SliceOutput sliceOutput)
    {
         System.out.println("Writing encoding Name " + NAME + " at " + sliceOutput.size());
        writeLengthPrefixedString(sliceOutput, NAME);
         System.out.println("Writing bufferedPositionCount(positionCount) " + bufferedPositionCount + " at " + sliceOutput.size());
        sliceOutput.writeInt(bufferedPositionCount);

         System.out.println("Writing nullsBuffer at " + sliceOutput.size());
        // TODO: When the buffers are requested from buffer pool, they would be linked lists of buffers, then we need to copy them one by one to sliceOutput.
        writeNullsTo(sliceOutput);

         System.out.println("Writing valuesBuffer at " + sliceOutput.size());
        sliceOutput.appendBytes(valuesBuffer, 0, valuesBufferIndex);
         System.out.println("Writing Block finishes at " + sliceOutput.size());
    }

    int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // NAME
                SIZE_OF_INT +  // positionCount
                SIZE_OF_BYTE + nullsBufferIndex + (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0) + // nulls uses 1 byte for mayHaveNull
                valuesBufferIndex;  // valuesBuffer
    }

    private void appendValuesToBuffer(Block block)
    {
        ensureValueBufferSize();
        if (block.mayHaveNull()) {
            appendBytesWithNullsToBuffer(block);
        }
        else {
            appendBytesToBuffer(block);
        }
    }

    private void appendBytesToBuffer(Block block)
    {
        //System.out.println("appendValuesToBuffer positionsOffset " + positionsOffset + " batchSize " + batchSize + " block size " + block.getPositionCount());
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            //System.out.println("i " + i + " positions[i] " + positions[i]);
            //try {
            byte value = block.getByte(positions[i]);
            ByteArrayUtils.writeByte(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;
//            }
//            catch (ArrayIndexOutOfBoundsException e) {
//                e.printStackTrace();
//            }
        }
    }

    private void appendBytesWithNullsToBuffer(Block block)
    {
        ensureValueBufferSize();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            byte value = block.getByte(position);
            ByteArrayUtils.writeByte(valuesBuffer, valuesBufferIndex, value);
            valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;
            if (!block.isNull(position)) {
                valuesBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }
    }

    private void ensureValueBufferSize()
    {
        int requiredSize = valuesBufferIndex + batchSize;
        if (requiredSize > valuesBuffer.length) {
            valuesBuffer = Arrays.copyOf(valuesBuffer, max(valuesBuffer.length * 2, requiredSize));
        }
    }
}

