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
package com.facebook.presto.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

public class LongArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "LONG_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position, 0));
            }
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }

    @Override
    int reserveBytesInBuffer(BlockContents contents, int rows, int offsetInBuffer, EncodingState state) {
        //  Reserves space for serialized 'rows' non-null longs
        // including headers. 5 for vallue count and null indicator, 4
        // for name character count + length of the name string.
        int size = 8 * rows + 5 + 4 + NAME.length;
        state.startInBuffer = offsetInBuffer;
        return offsetInBuffer + size;
    }

    @Override
    void addValues(BlockContents contents, int[] rows, int firstRow, int numRows, EncodingState state)
    {
                long[] longs = contents.longs;
                int[] map = content.rowNumberMap;
                valueOffset = state.valueOffset;
                        for (int i = firstRow; i < firstRow + numRows; i++) {
                    setLongUnchecked(state.topLevelSlice, valuesOffset + i *8, longs[map[rows[i]]]);
                    state.numValues += numRows;
                        }


    }
    
    @Override
    int getFinalSize(EncodingState state)
    {
        return 8 * state.numValues + (state.valueOffset - state.startInBuffer) + 5;
    }

    @Override
    void finish(EncodingState state, int newOffsetInBuffer, Slice buffer)
    {
        state.topLevelBuffer.setInt(state.valueOffset - 5, state.numValues);
        state.topLevelBuffer.setByte(state.valueOffset - 1, 0);
        if (buffer == state.topLevelBuffer && !state.anyNulls && state.startInBuffer == newStartInBuffer) {
            return;
        }
        int size = getFinalSize(state);
        System.arraycopy((byte[])state.topLevelBuffer.getBase(), state.startInBuffer, (byte[])buffer.getBase(), newStartInBuffer, size);;
    }
}
