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
        long position1 = sliceInput.position();
        // System.out.println("Reading positionCount at " + sliceInput.position());

        int positionCount = sliceInput.readInt();

        long position2 = sliceInput.position();
        // System.out.println("positionCount " + positionCount + " position " + position2);
        // assert(position1 + 4 == position2);

        // System.out.println("Reading nulls at " + position2);
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        long position3 = sliceInput.position();
        // assert(position2 + 1 == position3);

        // System.out.println("Reading values at " + position3);
        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }

        long position4 = sliceInput.position();
        // System.out.println("finished block slice postion at " + position4);
        // assert(position3 + positionCount * 8 == position4);

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }
}
