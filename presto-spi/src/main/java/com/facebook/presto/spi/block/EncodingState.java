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

public class EncodingState
{
    int numValues;
    int maxValues;
    boolean anyNulls;
    byte[] nullBits;
    Slice topLevelBuffer;
    int startInBuffer;
    int bytesInBuffer
        int valueOffset;
        Slice contentBuffer;
    long totalValues = 0;
    long totalBytes = 0;
    
    void reset(int maxValues, Slice buffer, int startInBuffer, String name)
    {
        numValues = 0;
        anyNulls = false;
        this.maxValues =maxValues;
        topLevelBuffer = buffer;
                byte[] bytes = name.getBytes(UTF_8);
                buffer.setInt(startInBuffer, bytes.length);
                buffer.setBytes(startInBuffer + 4, Slices.wrappedBuffer(bytes));
                
                valueOffset = startInBuffer + 4 + bytes.length;
    }
}
