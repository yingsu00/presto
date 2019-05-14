package com.facebook.presto.operator;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

public class OptimizedSliceOutput
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
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] buffer;
    public int size;

    OptimizedSliceOutput(int predefinedSize)
    {
        buffer = new byte[predefinedSize];
    }

    public void writeLong(long value)
    {
        unsafe.putLong(buffer, (long) size + ARRAY_BYTE_BASE_OFFSET, value);
        size += ARRAY_LONG_INDEX_SCALE;
    }

    public void writeLongAt(long value, int address)
    {
        unsafe.putLong(buffer, (long) address + ARRAY_BYTE_BASE_OFFSET, value);
    }

    public void increaseSize()
    {
        size += ARRAY_LONG_INDEX_SCALE;
    }

    public void writeLongOnCondition(long value, boolean condition)
    {
        unsafe.putLong(buffer, (long) size + ARRAY_BYTE_BASE_OFFSET, value);
        if (condition) {
            size += ARRAY_LONG_INDEX_SCALE;
        }
    }

    public void writeLongArray(long[] values, boolean[] nulls, int offset, int length)
    {
        for (int i = offset; i < offset + length; i++) {
            unsafe.putLong(buffer, (long) size + ARRAY_BYTE_BASE_OFFSET, values[i]);

            if (!nulls[i]) {
                size += ARRAY_LONG_INDEX_SCALE;
            }
        }
    }

    public int size()
    {
        return size;
    }

    public int capacity()
    {
        return buffer.length;
    }

    public void reset()
    {
        size = 0;
    }
}
