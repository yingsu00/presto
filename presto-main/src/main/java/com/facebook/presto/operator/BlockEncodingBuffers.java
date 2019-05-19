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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;

public abstract class BlockEncodingBuffers
{
    private static final int BITS_IN_BYTE = 8;
    private static final int MIN_NULLS_BUFFER_BYTES = 256 * 1024;

    int[] positions;
    int positionsOffset;  // each batch we copy the values of rows from positions[positionsOffset] to positions[positionsOffset + batchSize]
    int batchSize;
    int bufferedPositionCount;

    byte[] nullsBuffer;
    int nullsBufferIndex;  //The next byte address if new values to be added.
    boolean[] remainingNullsFromLastBatch = new boolean[BITS_IN_BYTE];
    int remainingNullsCount;
    boolean containsNull;

    static BlockEncodingBuffers createBlockEncodingBuffers(Block block, PartitionedOutputOperator.ColumnarObjectNode columnarBlock, int[] positions)
    {
        // TODO: Call the block decoder to flatten the Dictionary and RLE blocks first.
        switch (block.getEncodingName()) {
            case LongArrayBlockEncodingBuffers.NAME:
                return new LongArrayBlockEncodingBuffers(positions);
            case ArrayBlockEncodingBuffers.NAME:
                return new ArrayBlockEncodingBuffers(columnarBlock, positions);
            case VariableWidthBlockEncodingBuffers.NAME:
                return new VariableWidthBlockEncodingBuffers(positions);
            case ByteArrayBlockEncodingBuffers.NAME:
                return new ByteArrayBlockEncodingBuffers(positions);
            default:
                throw new IllegalArgumentException("Unsupported encoding: " + block.getEncodingName());
        }
    }

//    static BlockEncodingBuffers createBlockEncodingBuffers(Block block, int[] positions)
//    {
//        // TODO: Call the block decoder to flatten the Dictionary and RLE blocks first.
//
//        switch (block.getEncodingName()) {
//            case LongArrayBlockEncodingBuffers.NAME:
//                return new LongArrayBlockEncodingBuffers(positions);
//            case ArrayBlockEncodingBuffers.NAME:
//                return new ArrayBlockEncodingBuffers(block, positions);
//            case VariableWidthBlockEncodingBuffers.NAME:
//                return new VariableWidthBlockEncodingBuffers(positions);
//            default:
//                throw new IllegalArgumentException("Unsupported encoding: " + block.getEncodingName());
//        }
//    }

    abstract void prepareBuffers();

    abstract void resetBuffers();

    abstract void setColumnarObject(PartitionedOutputOperator.ColumnarObjectNode columnarObject);

    abstract void copyValues(Block block);

    abstract void writeTo(SliceOutput sliceOutput);

    abstract int getSizeInBytes();

    void setPositions(int[] positions)
    {
        this.positions = positions;
    }

    void setNextBatch(int positionsOffset, int batchSize)
    {
        checkArgument(positionsOffset + batchSize < positions.length);
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
    }

    void appendPositionRange(int offset, int addedLength)
    {
        //checkArgument(offset >= 0 && offset + addedLength < block.getPositionCount());
        int positionCount = positionsOffset + batchSize;
        ensurePositionsCapacity(positionCount + addedLength);

        for (int i = 0; i < addedLength; i++) {
            positions[positionCount + i] = offset + i;
        }

        batchSize += addedLength;
    }

    void appendNulls(Block block)
    {
        if (block.mayHaveNull()) {
            // We write to the buffer if there're potential nulls. Note that even though the block may contains null, the rows that go to this partition may not.
            // But we still write them to nullsBuffer anyways because of performance considerations.
            //System.out.println("1 " + nullsBuffer + " " + nullsBufferIndex + " " + remainingNullsCount + " " + bufferedPositionCount + " " + batchSize);
            ensureNullsValueBufferSize();
            int bufferedNullsCount = nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount;

            if (bufferedPositionCount > bufferedNullsCount) {
                // THere were no nulls for the rows in (bufferedNullsCount, bufferedPositionCount]. Backfill them as all 0's
                int falsesToWrite = bufferedPositionCount - bufferedNullsCount;
                //System.out.println("falsesToWrite " + falsesToWrite);
                encodeFalseValuesAsBits(falsesToWrite);
                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount);
            }
            //System.out.println("2 " + nullsBuffer + " " + nullsBufferIndex + " " + remainingNullsCount + " " + bufferedPositionCount + " " + batchSize);

            // Append this batch
            encodeNullsAsBits(block);

            verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount + batchSize);
            //System.out.println("3 " + nullsBuffer + " " + nullsBufferIndex + " " + remainingNullsCount + " " + bufferedPositionCount + " " + batchSize);
        }
        else {
            if (containsNull) {
                verify (nullsBufferIndex > 0);
                // There were nulls in previously buffered rows, but for this batch there can't be any nulls. Any how we need to append 0's for this batch.
                //System.out.println("4 " + nullsBuffer + " "  + nullsBufferIndex + " " + remainingNullsCount + " " + bufferedPositionCount + " " + batchSize);
                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount);

                ensureNullsValueBufferSize();
                encodeFalseValuesAsBits(batchSize);

                //System.out.println("5 " + nullsBuffer + " "  + nullsBufferIndex + " " + remainingNullsCount + " " + bufferedPositionCount + " " + batchSize);
                verify(nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount == bufferedPositionCount + batchSize);
            }
        }
    }

    void writeNullsTo(SliceOutput sliceOutput)
    {
        //System.out.println("writing nulls at " + sliceOutput.size());
        if (remainingNullsCount > 0) {
            encodeRemainingNullsAsBits();
            remainingNullsCount = 0;
        }

        if (containsNull) {
            verify((nullsBufferIndex - 1) * BITS_IN_BYTE < bufferedPositionCount && nullsBufferIndex * BITS_IN_BYTE >= bufferedPositionCount);
//            if (nullsBufferIndex * BITS_IN_BYTE < bufferedPositionCount || (nullsBufferIndex - 1) * BITS_IN_BYTE >= bufferedPositionCount) {
//                System.out.println("nullsBufferIndex " + nullsBufferIndex + " bufferedPositionCount " + bufferedPositionCount);
//            }
            sliceOutput.writeBoolean(true);
            sliceOutput.appendBytes(nullsBuffer, 0, nullsBufferIndex);
            //System.out.println("Written nulls. null length " + nullsBufferIndex + " now slice is at " + sliceOutput.size());
        }
        else {
            sliceOutput.writeBoolean(false);
            //System.out.println("No nulls, now slice is at " + sliceOutput.size());
        }
    }


    private void encodeNullsAsBits(Block block)
    {
        if (remainingNullsCount + batchSize < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int offset = positionsOffset; offset < positionsOffset + batchSize; offset++) {
                int position = positions[offset];
                remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(position);
            }
            return;
        }

        // Process the remaining nulls from last batch
        int offset = positionsOffset;

        if (remainingNullsCount > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int i = 0; i < remainingNullsCount; i++) {
                value |= remainingNullsFromLastBatch[i] ? mask : 0;
                mask >>>= 1;
            }

            // Process the next a few nulls to make up one byte
            int positionCountForFirstByte = BITS_IN_BYTE - remainingNullsCount;
            int endPositionOffset = positionsOffset + positionCountForFirstByte;
            while (offset < endPositionOffset) {
                //or (int offset = positionsOffset; offset < endPositionOffset; offset++) {
                int position = positions[offset];
                value |= block.isNull(position) ? mask : 0;
                mask >>>= 1;
                offset++;
            }
            containsNull |= (value != 0);
            ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex++, value);
        }

        // Process the next BITS_IN_BYTE * n positions. We now have processed (offset - positionsOffset) positions
        int remainingPositions = batchSize - (offset - positionsOffset);
        int positionsToEncode = remainingPositions & ~0b111;
        int endPositionOffset = offset + positionsToEncode;
        while (offset < endPositionOffset) {
            byte value = 0;
            value |= block.isNull(positions[offset]) ? 0b1000_0000 : 0;
            value |= block.isNull(positions[offset + 1]) ? 0b0100_0000 : 0;
            value |= block.isNull(positions[offset + 2]) ? 0b0010_0000 : 0;
            value |= block.isNull(positions[offset + 3]) ? 0b0001_0000 : 0;
            value |= block.isNull(positions[offset + 4]) ? 0b0000_1000 : 0;
            value |= block.isNull(positions[offset + 5]) ? 0b0000_0100 : 0;
            value |= block.isNull(positions[offset + 6]) ? 0b0000_0010 : 0;
            value |= block.isNull(positions[offset + 7]) ? 0b0000_0001 : 0;

            containsNull |= (value != 0);
            ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex, value);
            nullsBufferIndex++;
            offset += BITS_IN_BYTE;
        }

//        int positionsEncodedSoFar = positionCountForFirstByte + positionsToEncode;
//        remainingNullsCount = batchSize - positionsEncodedSoFar;
//        remainingNullsCount = 0;
//        endPositionOffset = positionsOffset + batchSize;
//        verify (offset > endPositionOffset - BITS_IN_BYTE);
//        while (offset < endPositionOffset) {
//            int position = positions[offset++];
//            remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(position);
//        }

        remainingPositions &= 0b111;
        verify (remainingPositions < BITS_IN_BYTE); //, format("%d", remainingPositions));
        remainingNullsCount = 0;
        while (remainingNullsCount < remainingPositions) {
            int position = positions[offset++];
            remainingNullsFromLastBatch[remainingNullsCount++] = block.isNull(position);
        }
    }

    private void encodeFalseValuesAsBits(int count)
    {
        if (remainingNullsCount + count < BITS_IN_BYTE) {
            // just put all of this batch to remainingNullsFromLastBatch
            for (int i = 0; i < count; i++) {
                remainingNullsFromLastBatch[remainingNullsCount++] = false;
            }
            return;
        }

        // Have to do this before calling encodeRemainingNullsAsBits() because it resets remainingNullsCount
        int remainingPositions = count;

        // Process the remaining nulls from last batch
        if (remainingNullsCount > 0) {
            encodeRemainingNullsAsBits();
            remainingPositions -= (BITS_IN_BYTE - remainingNullsCount);
            remainingNullsCount = 0;
        }

        int bytesCount = remainingPositions >>> 3;

        ByteArrayUtils.writeValues(nullsBuffer, nullsBufferIndex, bytesCount, (byte) 0);
        nullsBufferIndex += bytesCount;

        remainingPositions &= 0b111;
        remainingNullsCount = 0;
        while (remainingNullsCount < remainingPositions) {
            remainingNullsFromLastBatch[remainingNullsCount++] = false;
        }
    }

    private void encodeRemainingNullsAsBits()
    {
        byte value = 0;
        int mask = 0b1000_0000;
        for (int i = 0; i < remainingNullsCount; i++) {
            value |= remainingNullsFromLastBatch[i] ? mask : 0;
            mask >>>= 1;
        }

        containsNull |= (value != 0);
        ByteArrayUtils.writeByte(nullsBuffer, nullsBufferIndex, value);
        nullsBufferIndex++;
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

    private void ensureNullsValueBufferSize()
    {
        int requiredBytes = (bufferedPositionCount + batchSize) >>> 3 + 1;

        if (nullsBuffer == null) {
            int newSize = max(requiredBytes, MIN_NULLS_BUFFER_BYTES);
            ////System.out.println(newSize);
            nullsBuffer = new byte[newSize];
        }
        else if (nullsBuffer.length < requiredBytes){
            int newSize = max(requiredBytes, nullsBuffer.length * 2);
            ////System.out.println(newSize + ", nullsBuffer.length " + nullsBuffer.length);
            nullsBuffer = Arrays.copyOf(nullsBuffer, newSize);
        }
    }
}
