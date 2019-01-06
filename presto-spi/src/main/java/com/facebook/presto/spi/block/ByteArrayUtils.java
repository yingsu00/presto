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

import java.lang.reflect.Field;
import sun.misc.Unsafe;

import static sun.misc.Unsafe.ARRAY_BOOLEAN_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_DOUBLE_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_FLOAT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_LONG_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;
import static sun.misc.Unsafe.ARRAY_SHORT_INDEX_SCALE;


public class ByteArrayUtils
{
  static Unsafe unsafe;

  static {
    try {

      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
      if (unsafe == null) {
        throw new RuntimeException("Unsafe access not available");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

    public static long getLong(byte[] array, int offset)
    {
        return unsafe.getLong(array, ARRAY_BYTE_BASE_OFFSET + offset);
    }

        public static double getDouble(byte[] array, int offset)
    {
        return unsafe.getDouble(array, ARRAY_BYTE_BASE_OFFSET + offset);
    }

        public static int getInt(byte[] array, int offset)
    {
        return unsafe.getInt(array, ARRAY_BYTE_BASE_OFFSET + offset);
    }

    public static float getFloat(byte[] array, int offset)
    {
        return unsafe.getFloat(array, ARRAY_BYTE_BASE_OFFSET + offset);
    }
    
    public static short getShort(byte[] array, int offset)
    {
        return unsafe.getShort(array, ARRAY_BYTE_BASE_OFFSET + offset);
    }

    public static void gather( long[] source, int[] positions, int[] rowNumberMap, int sourceOffset, byte[] target, int targetOffset, int numWords)
    {
        if (target.length < targetOffset + numWords * ARRAY_LONG_INDEX_SCALE || targetOffset < 0) {
            throw new IndexOutOfBoundsException();
        }
        int end = sourceOffset + numWords;
        if (rowNumberMap == null) {
            for (int i = sourceOffset; i < end; i++) {
                unsafe.putLong(target, ARRAY_BYTE_BASE_OFFSET + targetOffset + i * ARRAY_LONG_INDEX_SCALE, source[positions[i]]); 
            }
        }
        else {
                        for (int i = sourceOffset; i < end; i++) {
                unsafe.putLong(target, ARRAY_BYTE_BASE_OFFSET + targetOffset + i * ARRAY_LONG_INDEX_SCALE, source[rowNumberMap[positions[i]]]); 
            }
        }
    }

    public static void copyToLongs(byte[] source, int sourceOffset, long[] destination, int destinationIndex, int numWords)
    {
        if (destination.length < destinationIndex + numWords || destinationIndex < 0 || source.length < sourceOffset + numWords * ARRAY_LONG_INDEX_SCALE || sourceOffset < 0) {
            throw new IndexOutOfBoundsException();
        }
        unsafe.copyMemory(source, ARRAY_BYTE_BASE_OFFSET + sourceOffset, destination, ARRAY_LONG_BASE_OFFSET + destinationIndex * ARRAY_LONG_INDEX_SCALE, numWords * ARRAY_LONG_INDEX_SCALE);
    }
}
