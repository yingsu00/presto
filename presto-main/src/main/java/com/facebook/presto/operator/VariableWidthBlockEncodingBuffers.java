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

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import com.facebook.presto.spi.block.Block;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static java.lang.String.format;
import static sun.misc.Unsafe.ARRAY_BYTE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class VariableWidthBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "VARIABLE_WIDTH";
    public static final int DEFAULT_MAX_ELEMENT_COUNT = 128 * 1024;

    // These are single piece buffers for now. They will be requested from buffer pool dynamically
    private byte[] sliceBuffer;
    private int sliceBufferIndex;

    private byte[] nullsBuffer;
    private int nullsBufferIndex;

    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;

    public VariableWidthBlockEncodingBuffers(int[] positions)
    {
        this.positions = positions;
        prepareBuffers();
    }

    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
    }

    @Override
    protected void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future
        if (sliceBuffer == null) {
            sliceBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE];
        }
        sliceBufferIndex = 0;

        if (nullsBuffer == null) {
            nullsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT / ARRAY_BYTE_INDEX_SCALE];
        }
        nullsBufferIndex = 0;

        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;
    }

    @Override
    public void copyValues(Block block)
    {
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 && positions[positionsOffset + batchSize - 1] < block.getPositionCount(),
                format("positionOffset %d + batchSize %d - 1 should be less than the positions array size %d, " +
                                "and the start position %d in the positions array should be greater than or equal to 0, " +
                                "and the largest position %d in the positions array should be smaller than block positionCount %d.",
                        positionsOffset,
                        batchSize,
                        positions.length,
                        positions[positionsOffset],
                        positions[positionsOffset + batchSize - 1],
                        block.getPositionCount()));

        appendOffsetsAndSlices(block);
        bufferedPositionCount += batchSize;
    }
//
//    public void appendNulls(boolean mayHaveNull, boolean[] nulls, int offsetBase)
//    {
//        //TODO: ensure nullsBuffer has enough space for these rows.
//        if (mayHaveNull) {
//            nullsBufferIndex = ByteArrayUtils.encodeNullsAsBits(nulls, positions, positionsOffset, batchSize, nullsBuffer, nullsBufferIndex);
//        }
//    }

//    private void appendOffsetsAndSlices(Block block)
//    {
//        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE,
//                format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));
//
//        int lastOffset = 0;
//        if (offsetsBufferIndex > 0) {
//            // There're already some values in the buffer
//            lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
//        }
//
//        ensureOffsetsBufferSize();
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int beginOffsetInBlock = variableWidthBlock.getPositionOffset(position);
//            int endOffsetInBlock = variableWidthBlock.getPositionOffset(position + 1);
//            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
//            int currentOffset = lastOffset + currentRowSize;
//
//
//            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            //System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i + " position " + position);
//
//            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));
//
//            lastOffset = currentOffset;
//        }
//    }

    private void appendOffsetsAndSlices(Block block)
    {
        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE,
                format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));

        int lastOffset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
        }

        ensureOffsetsBufferSize();

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int currentRowSize = variableWidthBlock.getSliceLength(position);
            int currentOffset = lastOffset + currentRowSize;


            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;

            //System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i + " position " + position);

            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));

            lastOffset = currentOffset;
        }
    }

    public void writeTo(SliceOutput sliceOutput)
    {
        // System.out.println("Writing encoding Name " + NAME + " at " + sliceOutput.size());
        writeLengthPrefixedString(sliceOutput, NAME);
        // System.out.println("Writing bufferedPositionCount(positionCount) " + bufferedPositionCount + " at " + sliceOutput.size());
        sliceOutput.writeInt(bufferedPositionCount);

        // offsets
        //sliceOutput.writeInt(0);  // the base position
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        // System.out.println("Writing nullsBuffer at " + sliceOutput.size());
        // nulls
        // TODO: When the buffers are requested from buffer pool, they would be linked lists of buffers, then we need to copy them one by one to sliceOutput.
        if (nullsBufferIndex > 0) {
            sliceOutput.writeBoolean(true);
            sliceOutput.appendBytes(nullsBuffer, 0, nullsBufferIndex);
        }
        else {
            sliceOutput.writeBoolean(false);
        }

        // System.out.println("Writing sliceBuffer at " + sliceOutput.size());
        sliceOutput.writeInt(sliceBufferIndex);  // totalLength
        sliceOutput.appendBytes(sliceBuffer, 0, sliceBufferIndex);
        // System.out.println("Writing Block finishes at " + sliceOutput.size());
    }

    public int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // NAME
                SIZE_OF_INT +  // positionCount
                nullsBufferIndex + 1 + // nulls uses 1 byte for mayHaveNull
                SIZE_OF_INT + offsetsBufferIndex + // offsets
                sliceBufferIndex;  // sliceBuffer
    }

    private void appendValuesWithNullsToBuffer(Block block)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            long value = block.getLong(position);
            ByteArrayUtils.writeLong(sliceBuffer, sliceBufferIndex, value);
            if (block.isNull(position)) {
                sliceBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }
    }

    private void ensureSliceBufferSize()
    {
        int requiredSize = sliceBufferIndex + batchSize * ARRAY_LONG_INDEX_SCALE;
        if (requiredSize > sliceBuffer.length) {
            sliceBuffer = Arrays.copyOf(sliceBuffer, max(sliceBuffer.length * 2, requiredSize));
        }
    }

    private void ensureOffsetsBufferSize()
    {
        int requiredSize = offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE;
        if (requiredSize > offsetsBuffer.length) {
            offsetsBuffer = Arrays.copyOf(offsetsBuffer, max(requiredSize, offsetsBuffer.length * 2));
        }
    }
}
