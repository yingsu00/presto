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

import com.facebook.presto.spi.block.ArrayBlockEncoding;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlockEncoding;
import io.airlift.slice.SliceOutput;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.String.format;

public abstract class BlockEncodingBuffers
{
    protected int[] positions;

    protected int positionsOffset;  // each batch we copy the values of rows from positions[positionsOffset] to positions[positionsOffset + batchSize]
    protected int batchSize;
    protected int bufferedPositionCount;

    protected abstract void prepareBuffers();

    public abstract void resetBuffers();

    public abstract void copyValues(Block block);

    public void setPositions(int[] positions)
    {
        this.positions = positions;
    }

    public abstract void writeTo(SliceOutput sliceOutput);

    public abstract int getSizeInBytes();

//    abstract void appendFixedWidthValues(Object values, boolean[] nulls, boolean mayHaveNull, int offsetBase);
//
//    abstract void appendNulls(boolean mayHaveNull, boolean[] nulls, int offsetBase);
//
//    abstract void appendOffsets(int[] offsets, int offsetBase);
//
//    abstract void appendBlock(Block block, int offsetBase);

    public void setNextBatch(int positionsOffset, int batchSize)
    {
        //checkArgument(positionsOffset + batchSize < positions.length);
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
    }

    protected void appendPositionRange(int offset, int addedLength)
    {
        //checkArgument(offset >= 0 && offset + addedLength < block.getPositionCount());
        int positionCount = positionsOffset + batchSize;
        ensurePositionsCapacity(positionCount + addedLength);

        for (int i = 0; i < addedLength; i++) {
            positions[positionCount + i] = offset + i;
        }

        batchSize += addedLength;
    }

    private void ensurePositionsCapacity(int length)
    {
        if (this.positions == null) {
            positions = new int[length];
        }
        else if (this.positions.length < length) {
//            int[] newPositions = new int[max(positions.length * 2, length)];
//            System.arraycopy(positions, 0, newPositions, 0, positionCount);
//            positions = newPositions;
            positions = Arrays.copyOf(positions, max(positions.length * 2, length));
        }
    }

    static BlockEncodingBuffers createBlockEncodingBuffers(Block block, int[] positions)
    {
        // TODO: Call the block decoder to flatten the Dictionary and RLE blocks first.

        switch (block.getEncodingName()) {
            case LongArrayBlockEncodingBuffers.NAME:
                return new LongArrayBlockEncodingBuffers(positions);
            case ArrayBlockEncodingBuffers.NAME:
                return new ArrayBlockEncodingBuffers(block, positions);
            case VariableWidthBlockEncodingBuffers.NAME:
                return new VariableWidthBlockEncodingBuffers(positions);
            default:
                throw new IllegalArgumentException("Unsupported encoding: " + block.getEncodingName());
        }
    }

    public void updateBufferedPositionCount()
    {
        bufferedPositionCount += batchSize;
    }
}
