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
package com.facebook.presto.parquet.batchreader.decoders;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TestParquetUtils
{
    private TestParquetUtils()
    {
    }

    public static RunLengthBitPackingHybridEncoder getSimpleDLEncoder()
    {
        return new RunLengthBitPackingHybridEncoder(1, 200, 1024 * 1000, new HeapByteBufferAllocator());
    }

    public static RunLengthBitPackingHybridEncoder getDictionaryDataPageEncoder(int maxValue)
    {
        return new RunLengthBitPackingHybridEncoder(BytesUtils.getWidthFromMaxInt(maxValue), 200, 1024 * 1000, new HeapByteBufferAllocator());
    }

    public static void addDLRLEBlock(int rleValue, int valueCount, RunLengthBitPackingHybridEncoder encoder, List<Integer> addedValues)
    {
        checkArgument(valueCount >= 8, "Requires value count to be greater than 8 for RLE block");
        try {
            for (int i = 0; i < valueCount; i++) {
                encoder.writeInt(rleValue);
                addedValues.add(rleValue);
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static void addDLValues(Iterator<Integer> values, RunLengthBitPackingHybridEncoder encoder, List<Integer> addedValues)
    {
        try {
            while (values.hasNext()) {
                int value = values.next();
                encoder.writeInt(value);
                addedValues.add(value);
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Iterator<Integer> randomValues(Random random, int numValues, int maxValue)
    {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < numValues; i++) {
            values.add(random.nextInt(maxValue + 1));
        }
        return values.iterator();
    }

    public static byte[] generatePlainValuesPage(int valueCount, int valueSize, Random random, List<Object> addedValues)
    {
        PlainValuesWriter writer = new PlainValuesWriter(20, 1024 * 1000, new HeapByteBufferAllocator());

        switch (valueSize) {
            case 4: {
                for (int i = 0; i < valueCount; i++) {
                    int value = random.nextInt();
                    writer.writeInteger(value);
                    addedValues.add(value);
                }
                break;
            }
            case 8: {
                for (int i = 0; i < valueCount; i++) {
                    long value = random.nextLong();
                    writer.writeLong(value);
                    addedValues.add(value);
                }
                break;
            }
            default:
                throw new IllegalArgumentException("invalid value size (expected: 4, 8 or 12)");
        }

        try {
            return writer.getBytes().toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] generateDictionaryIdPage2048(int maxValue, Random random, List<Integer> addedValues)
    {
        RunLengthBitPackingHybridEncoder encoder = getDictionaryDataPageEncoder(maxValue);

        addDLRLEBlock(maxValue / 2, 50, encoder, addedValues);
        addDLValues(randomValues(random, 457, maxValue), encoder, addedValues);
        addDLRLEBlock(0, 37, encoder, addedValues);
        addDLValues(randomValues(random, 186, maxValue), encoder, addedValues);
        addDLValues(randomValues(random, 289, maxValue), encoder, addedValues);
        addDLRLEBlock(maxValue - 1, 76, encoder, addedValues);
        addDLValues(randomValues(random, 789, maxValue), encoder, addedValues);
        addDLRLEBlock(maxValue - 1, 137, encoder, addedValues);
        addDLValues(randomValues(random, 27, maxValue), encoder, addedValues);

        checkState(addedValues.size() == 2048);

        try {
            return encoder.toBytes().toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
