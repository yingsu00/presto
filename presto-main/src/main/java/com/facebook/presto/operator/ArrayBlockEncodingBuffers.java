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

import com.facebook.presto.spi.block.AbstractArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.ColumnarArray;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.facebook.presto.operator.ByteArrayUtils.writeLengthPrefixedString;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class ArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    public static final String NAME = "ARRAY";
    public static final int DEFAULT_MAX_ELEMENT_COUNT = 8 * 1024;

    // TODO: These are single piece buffers for now. They will become linked list of pages requested from buffer pool.
    private byte[] offsetsBuffer;
    private int offsetsBufferIndex;  //The next byte address if new positions to be added.

    private ColumnarArray columnarArray;
    private BlockEncodingBuffers rawBlockBuffer;

    ArrayBlockEncodingBuffers(PartitionedOutputOperator.ColumnarObjectNode columnarObjectNode, int[] positions)
    {
        this.positions = positions;
        ColumnarArray columnarArray = (ColumnarArray)columnarObjectNode.columnarObject;
        Block rawBlock = columnarArray.getElementsBlock();

        int[] nestedLevelPositions = new int[positions.length * 2];
        rawBlockBuffer = createBlockEncodingBuffers(rawBlock, columnarObjectNode.getChild(0), nestedLevelPositions);
        prepareBuffers();
    }

//    ArrayBlockEncodingBuffers(Block block, int[] positions)
//    {
//        this.positions = positions;
//        Block rawBlock = toColumnarArray(block).getElementsBlock();
//
//        int[] nestedLevelPositions = new int[positions.length * 2];
//        rawBlockBuffer = createBlockEncodingBuffers(rawBlock, nestedLevelPositions);
//        prepareBuffers();
//    }

    @Override
    void prepareBuffers()
    {
        // TODO: These local buffers will be requested from the buffer pools in the future.
        if (offsetsBuffer == null) {
            offsetsBuffer = new byte[DEFAULT_MAX_ELEMENT_COUNT * ARRAY_LONG_INDEX_SCALE];
        }
        offsetsBufferIndex = 0;

        rawBlockBuffer.prepareBuffers();
    }

    @Override
    void resetBuffers()
    {
        bufferedPositionCount = 0;
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        containsNull = false;
        offsetsBufferIndex = 0;
        rawBlockBuffer.resetBuffers();
    }

    void setColumnarObject(PartitionedOutputOperator.ColumnarObjectNode columnarBLock)
    {
        columnarArray = (ColumnarArray) columnarBLock.columnarObject;
        PartitionedOutputOperator.ColumnarObjectNode childNode = columnarBLock.getChild(0);
        if (childNode != null) {
            rawBlockBuffer.setColumnarObject(childNode);
        }
    }

    @Override
    void copyValues(Block block)
    {
        verify(positionsOffset + batchSize - 1 < positions.length && positions[positionsOffset] >= 0 && positions[positionsOffset + batchSize - 1] < block.getPositionCount());
//                format("positionOffset %d + batchSize %d - 1 should be less than the positions array size %d, " +
//                        "and the start position %d in the positions array should be greater than or equal to 0, " +
//                        "and the largest position %d in the positions array should be smaller than block positionCount %d.",
//                        positionsOffset,
//                        batchSize,
//                        positions.length,
//                        positions[positionsOffset],
//                        positions[positionsOffset + batchSize - 1],
//                        block.getPositionCount()));

        appendOffsets(block);
        appendNulls(block);

        rawBlockBuffer.copyValues(columnarArray.getElementsBlock());

        bufferedPositionCount += batchSize;
    }

    void writeTo(SliceOutput sliceOutput)
    {
        writeLengthPrefixedString(sliceOutput, NAME);
        //

        // TODO: When the buffers are requested from buffer pool, they would be linked lists of buffers, then we need to copy them one by one to sliceOutput.
        rawBlockBuffer.writeTo(sliceOutput);

        sliceOutput.writeInt(bufferedPositionCount); //positionCount

        sliceOutput.writeInt(0);  // the base position
        sliceOutput.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);

        writeNullsTo(sliceOutput);
    }

    int getSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +  // encoding name
                rawBlockBuffer.getSizeInBytes() +
                SIZE_OF_INT +  // positionCount
                SIZE_OF_INT + offsetsBufferIndex + // offsets. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                SIZE_OF_BYTE + nullsBufferIndex + (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0); //nulls
    }

    private void appendOffsets(Block block)
    {
        // Nested level positions always start from 0.
        rawBlockBuffer.setNextBatch(0, 0);

        verify(offsetsBufferIndex == bufferedPositionCount * ARRAY_INT_INDEX_SCALE);
//                format("offsetsBufferIndex %d should equal to bufferedPositionCount %d * ARRAY_INT_INDEX_SCALE %d.", offsetsBufferIndex, bufferedPositionCount, ARRAY_INT_INDEX_SCALE));

        int lastOffset = 0;
        if (offsetsBufferIndex > 0) {
            // There're already some values in the buffer
            lastOffset = ByteArrays.getInt(offsetsBuffer, offsetsBufferIndex - ARRAY_INT_INDEX_SCALE);
        }

        ensureOffsetsBufferSize();

        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int beginOffsetInBlock = arrayBlock.getOffset(position);
            int endOffsetInBlock = arrayBlock.getOffset(position + 1);
            int currentRowSize = endOffsetInBlock - beginOffsetInBlock;
            int currentOffset = lastOffset + currentRowSize;


            ByteArrayUtils.writeInt(offsetsBuffer, offsetsBufferIndex, currentOffset);
            offsetsBufferIndex += ARRAY_INT_INDEX_SCALE;

            //System.out.println("positionsOffset " + positionsOffset + " batchSize " + batchSize + " i " + i + " position " + position);

            // One row correspond to a range of rows of the next level. Add these row positions to the next level positions
            rawBlockBuffer.appendPositionRange(beginOffsetInBlock, currentRowSize);

            lastOffset = currentOffset;
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
