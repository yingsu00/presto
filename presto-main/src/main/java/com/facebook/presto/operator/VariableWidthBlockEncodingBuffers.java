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
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static java.lang.String.format;
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

//    private byte[] nullsBuffer;
//    private int nullsBufferIndex;

    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;

    VariableWidthBlockEncodingBuffers(int[] positions)
    {
        this.positions = positions;
        prepareBuffers();
    }

    @Override
    void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future
        if (sliceBuffer == null) {
            sliceBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE * 2];
        }
        sliceBufferIndex = 0;

//        if (nullsBuffer == null) {
//            nullsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT / ARRAY_BYTE_INDEX_SCALE];
//        }
//        nullsBufferIndex = 0;

        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_INT_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;
    }

    void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        containsNull = false;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
    }

    void setColumnarObject(PartitionedOutputOperator.ColumnarObjectNode columnarObjectNode)
    {
        throw new UnsupportedOperationException("VariableWidthBlockEncodingBuffers does not support setColumnarObject");
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

        appendOffsetsAndSlices(block);
        appendNulls(block);
        bufferedPositionCount += batchSize;
    }

    void writeTo(SliceOutput sliceOutput)
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
        writeNullsTo(sliceOutput);

        // System.out.println("Writing sliceBuffer at " + sliceOutput.size());
        sliceOutput.writeInt(sliceBufferIndex);  // totalLength
        sliceOutput.appendBytes(sliceBuffer, 0, sliceBufferIndex);
        // System.out.println("Writing Block finishes at " + sliceOutput.size());
    }

    // Slowest
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
//
    // Fastest
//    private void appendOffsetsAndSlices(Block block)
//    {
//        //System.out.println(format("bufferedPositionCount %d offsetsBufferIndex %d offsetsBufferIndex.length %d sliceBufferIndex %d sliceBuffer.length %d", bufferedPositionCount, offsetsBufferIndex, offsetsBuffer.length, sliceBufferIndex, sliceBuffer.length));
//        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
//               // format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));
//
//        int lastOffset = 0;
//        if (offsetsBufferIndex > 0) {
//            // There're already some values in the buffer
//            //System.out.println(offsetsBufferIndex);
//            try {
//                lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        ensureOffsetsBufferSize();
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
//        int startingOffset = lastOffset;
//        int totalLength = 0;
//        int startingOffsetBufferIndex = offsetsBufferIndex;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int currentRowSize = variableWidthBlock.getSliceLength(position);
//            int currentOffset = lastOffset + currentRowSize;
//            totalLength += currentRowSize;
//
//            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            lastOffset = currentOffset;
//        }
//
//        ensureSliceBufferSize(totalLength);
//
//        int index = startingOffsetBufferIndex;
//        lastOffset = startingOffset;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int currentOffset = ByteArrays.getInt(offsetsBuffer, index);
//            int currentRowSize = currentOffset - lastOffset;
//            // System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i +
//              //      " position " + position + " sliceBufferIndex " + sliceBufferIndex + " slice.length " + currentRowSize);
//
//            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));
//
//            index += ARRAY_INT_INDEX_SCALE;
//            lastOffset = currentOffset;
//        }
//    }

    // buggy 1513ms
    private void appendOffsetsAndSlices(Block block)
    {
        //System.out.println(format("bufferedPositionCount %d offsetsBufferIndex %d offsetsBufferIndex.length %d sliceBufferIndex %d sliceBuffer.length %d", bufferedPositionCount, offsetsBufferIndex, offsetsBuffer.length, sliceBufferIndex, sliceBuffer.length));
        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
        // format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));

        int lastOffset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            //System.out.println(offsetsBufferIndex);
            try {
                lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        ensureOffsetsBufferSize();

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
        int sliceLength = variableWidthBlock.getPositionOffset(variableWidthBlock.getPositionCount());

        int startingOffset = lastOffset;
        int totalLength = 0;
        int startingOffsetBufferIndex = offsetsBufferIndex;
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
//            int beginOffsetInBlock = variableWidthBlock.getPositionOffset(position);
//            int endOffsetInBlock = variableWidthBlock.getPositionOffset(position + 1);
//            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
            int currentRowSize = variableWidthBlock.getSliceLength(position);
            int currentOffset = lastOffset + currentRowSize;
            totalLength += currentRowSize;

            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;

            lastOffset = currentOffset;
        }

        ensureSliceBufferSize(totalLength);

        byte[] sliceBase = (byte[]) variableWidthBlock.getSlice(0, 0, sliceLength).getBase();

        int index = startingOffsetBufferIndex;
        lastOffset = startingOffset;
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int currentOffset = ByteArrays.getInt(offsetsBuffer, index);
            int currentRowSize = currentOffset - lastOffset;
            // System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i +
            //      " position " + position + " sliceBufferIndex " + sliceBufferIndex + " slice.length " + currentRowSize);


            sliceBufferIndex = ByteArrayUtils.copyBytes(sliceBuffer, sliceBufferIndex, sliceBase, variableWidthBlock.getPositionOffset(position), currentRowSize);

            index += ARRAY_INT_INDEX_SCALE;
            lastOffset = currentOffset;
        }
    }

    // 1518
//    private void appendOffsetsAndSlices(Block block)
//    {
//        //System.out.println(format("bufferedPositionCount %d offsetsBufferIndex %d offsetsBufferIndex.length %d sliceBufferIndex %d sliceBuffer.length %d", bufferedPositionCount, offsetsBufferIndex, offsetsBuffer.length, sliceBufferIndex, sliceBuffer.length));
//        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
//        // format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));
//
//        int lastOffset = 0;
//        if (offsetsBufferIndex > 0) {
//            // There're already some values in the buffer
//            //System.out.println(offsetsBufferIndex);
//            try {
//                lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        ensureOffsetsBufferSize();
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
//        int sliceLength = variableWidthBlock.getPositionOffset(variableWidthBlock.getPositionCount());
//        byte[] sliceBase = (byte[]) variableWidthBlock.getSlice(0, 0, sliceLength).getBase();
//
//        int startingOffset = lastOffset;
//        //int totalLength = 0;
//        int startingOffsetBufferIndex = offsetsBufferIndex;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int beginOffsetInBlock = variableWidthBlock.getPositionOffset(position);
//            int endOffsetInBlock = variableWidthBlock.getPositionOffset(position + 1);
//            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
//            //int currentRowSize = variableWidthBlock.getSliceLength(position);
//            int currentOffset = lastOffset + currentRowSize;
//            //totalLength += currentRowSize;
//
//            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            ensureSliceBufferSize(currentRowSize);
//
//            sliceBufferIndex = ByteArrayUtils.copyBytes(sliceBuffer, sliceBufferIndex, sliceBase, beginOffsetInBlock, currentRowSize);
//
//            lastOffset = currentOffset;
//        }
//    }
//
    // 1870
//    private void appendOffsetsAndSlices(Block block)
//    {
//        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
//        // format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));
//
//        int lastOffset = 0;
//        if (offsetsBufferIndex > 0) {
//            // There're already some values in the buffer
//            //System.out.println(offsetsBufferIndex);
//            try {
//                lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//
//        ensureOffsetsBufferSize();
//
//        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;
//        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
//            int position = positions[i];
//            int currentRowSize = variableWidthBlock.getSliceLength(position);
//            int currentOffset = lastOffset + currentRowSize;
//
//
//            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
//            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;
//
//            // System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i +
//            //      " position " + position + " sliceBufferIndex " + sliceBufferIndex + " slice.length " + currentRowSize);
//
//            ensureSliceBufferSize(currentRowSize);
//            sliceBufferIndex = ByteArrayUtils.writeSlice(sliceBuffer, sliceBufferIndex, variableWidthBlock.getSlice(position, 0, currentRowSize));
//
//            lastOffset = currentOffset;
//        }
//    }


    int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // NAME
                SIZE_OF_INT +  // positionCount
                SIZE_OF_BYTE + nullsBufferIndex + (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0) + // nulls uses 1 byte for mayHaveNull
                SIZE_OF_INT + offsetsBufferIndex + // offsets
                sliceBufferIndex;  // sliceBuffer
    }

    private void ensureSliceBufferSize(int currentRowSize)
    {
        int requiredSize = sliceBufferIndex + currentRowSize;
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
