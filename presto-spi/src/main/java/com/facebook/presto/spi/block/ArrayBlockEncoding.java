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
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.block.ArrayBlock.createArrayBlockInternal;
import static com.facebook.presto.spi.block.EncoderUtil.decodeNullBits;
import static com.facebook.presto.spi.block.EncoderUtil.encodeNullsAsBits;

public class ArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;

        int positionCount = arrayBlock.getPositionCount();

        int offsetBase = arrayBlock.getOffsetBase();
        int[] offsets = arrayBlock.getOffsets();

        int valuesStartOffset = offsets[offsetBase];
        int valuesEndOffset = offsets[offsetBase + positionCount];
        Block values = arrayBlock.getRawElementBlock().getRegion(valuesStartOffset, valuesEndOffset - valuesStartOffset);
        blockEncodingSerde.writeBlock(sliceOutput, values);

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(offsets[offsetBase + position] - valuesStartOffset);
        }
        encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        //System.out.println("Reading values at " + sliceInput.position());
        Block values = blockEncodingSerde.readBlock(sliceInput);

        //System.out.println("Reading positionCount at " + sliceInput.position());
        int positionCount = sliceInput.readInt();
        //System.out.println("positionCount " + positionCount);

        int[] offsets = new int[positionCount + 1];
        //System.out.println("Reading offsets at " + sliceInput.position());
        sliceInput.readBytes(Slices.wrappedIntArray(offsets));
        //System.out.println("Finished reading offsets at " + sliceInput.position());

        //System.out.println("Reading nulls at " + sliceInput.position());
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);
        return createArrayBlockInternal(0, positionCount, valueIsNull, offsets, values);
    }
}
