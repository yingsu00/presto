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

import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.util.Objects.requireNonNull;

public final class ConcatenatedByteArrayInputStream
        extends FixedLengthSliceInput
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcatenatedByteArrayInputStream.class).instanceSize();

    private ArrayList<byte[]> buffers;
    private byte[] current;
    private int currentIdx;
    private long currentSize;
    private long position;
    private long totalSize;
    private ByteArrayAllocator allocator;
    private boolean freeAfterRead = false;
    byte[] tempBytes = new byte[8];
    long previousBuffersSize = 0;
    
    public ConcatenatedByteArrayInputStream(List<byte[]> buffers, int size, ByteArrayAllocator allocator)
    {
        this.buffers = new ArrayList(buffers);
        for (byte[] buffer : buffers) {
            this.buffers.add(buffer);
        }
        this.allocator = allocator; 
        position = 0;
        currentIdx = -1;
        nextBuffer(0);
    }

    private void nextBuffer(int dataSize)
    {
        int numInCurrent = 0;
        if (dataSize > 0) {
            numInCurrent = (int)(currentSize - position);
            System.arraycopy(current, (int)position, tempBytes, 0, numInCurrent);
        }
        if (currentIdx >= 0 && freeAfterRead) {
            allocator.free(buffers.get(currentIdx));
            buffers.set(currentIdx, null);
        }
        previousBuffersSize += currentSize;
        currentIdx++;
        
        if (currentIdx >= buffers.size()) {
            if (dataSize - numInCurrent > 0) {
                throw new IndexOutOfBoundsException();
            }
            current = null;
            currentSize = 0;
            return;
        }
        current = buffers.get(currentIdx);
        if (currentIdx == buffers.size() - 1) {
            currentSize = totalSize - previousBuffersSize;
        } else {
            currentSize = current.length;
        }
        position = 0;
        if (dataSize > numInCurrent) {
            System.arraycopy(current, 0, tempBytes, numInCurrent, dataSize - numInCurrent);
            position = dataSize - numInCurrent;
        }
    }

    @Override
    public long length()
    {
        return totalSize;
    }

    @Override
    public long position()
    {
        return previousBuffersSize + position;
    }

    @Override
    public void setPosition(long position)
    {
        if (freeAfterRead && position < previousBuffersSize || position > totalSize) {
            throw new IndexOutOfBoundsException();
        }
        if (position > previousBuffersSize && position - previousBuffersSize < currentSize) {
            this.position =  position - previousBuffersSize;
            return;
        }
        if (!freeAfterRead) {
            previousBuffersSize = 0;
        }
        for (int i = 0; i < buffers.size(); i++) {
            byte[] buffer = buffers.get(i);
            if (buffer == null) {
                continue;
            }
            currentIdx = i;
            currentSize = buffer.length;
            nextBuffer(0);
            if (position >= previousBuffersSize && position < previousBuffersSize + currentSize) {
                this.position = position - previousBuffersSize;
                return;
            }
        }
        if (position > totalSize) {
            throw new IndexOutOfBoundsException();
        }
    }

    @Override
    public boolean isReadable()
    {
        return position < currentSize || currentIdx < buffers.size() - 1;
    }

    @Override
    public int available()
    {
        return (int)(totalSize - previousBuffersSize - position);
    }

    public int contiguousAvailable()
    {
        return (int)(currentSize - position);
    }

    byte[] getBuffer()
    {
        return current;
    }
    
    int getOffsetInBuffer()
    {
        return (int)position;
    }

    @Override
    public boolean readBoolean()
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        if (position >= currentSize) {
            nextBuffer(0);
            if (current == null) {
                return -1;
            }
            }
        int result = current[(int)position] & 0xff;
        position++;
        return result;
    }

    @Override
    public byte readByte()
    {
        int value = read();
        if (value == -1) {
            throw new IndexOutOfBoundsException();
        }
        return (byte) value;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        long newPosition = position + SIZE_OF_SHORT;
        short v;
        if (newPosition < currentSize) {
            v = ByteArrayUtils.getShort(current, (int)position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_SHORT);
            v = ByteArrayUtils.getShort(tempBytes, 0);
        }
        return v;
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        long newPosition = position + SIZE_OF_INT;
        int v;
        if (newPosition < currentSize) {
            v = ByteArrayUtils.getInt(current, (int)position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_INT);
            v = ByteArrayUtils.getInt(tempBytes, 0);
        }
        return v;
    }

    @Override
    public long readLong()
    {
        long newPosition = position + SIZE_OF_LONG;
        long v;
        if (newPosition < currentSize) {
            v = ByteArrayUtils.getLong(current, (int)position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_LONG);
            v = ByteArrayUtils.getLong(tempBytes, 0);
        }
        return v;
    }

    @Override
    public float readFloat()
    {
        long newPosition = position + SIZE_OF_FLOAT;
        float v;
        if (newPosition < currentSize) {
            v = ByteArrayUtils.getFloat(current, (int)position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_FLOAT);
            v = ByteArrayUtils.getFloat(tempBytes, 0);
        }
        return v;
    }

@Override
    public double readDouble()
    {
        long newPosition = position + SIZE_OF_DOUBLE;
        double v; 
        if (newPosition < currentSize) {
            v = ByteArrayUtils.getDouble(current, (int)position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_DOUBLE);
            v = ByteArrayUtils.getFloat(tempBytes, 0);
        }
        return v;
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        if (!freeAfterRead && currentSize - position <= length) {
            Slice v = Slices.wrappedBuffer(current, (int)position, length);
            skip(length);
            return v;
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return Slices.wrappedBuffer(bytes);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        if (length == 0) {
            return 0;
        }
        length = Math.min(length, available());
        if (length == 0) {
            return -1;
        }
        readBytes(destination, destinationIndex, length);
        return length;
    }

@Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        while (length > 0) {
            int copy = (int)Math.min(length, currentSize - position);
            if (copy == 0) {
                nextBuffer(0);
                if (current == null) {
                    throw new IndexOutOfBoundsException();
                }
                continue;
            }
            System.arraycopy(current, (int)position, destination, destinationIndex, copy);
        position += copy;
        destinationIndex += copy;
        length -= copy;
        }
        if (position == currentSize) {
            nextBuffer(0);
        }
    }

@Override
public void readBytes(Slice destination, int destinationIndex, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long length)
    {
        length = Math.min(length, available());
        long toGo = length;
        while (toGo > 0) {
            if (toGo < currentSize - position) {
                position += toGo;
                break;
            }
            toGo -= currentSize - position;
            nextBuffer(0);
        }
        return length;
    }

    @Override
    public int skipBytes(int length)
    {
        return (int)skip(length);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + totalSize;
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public Slice slice()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.  This method is identical to
     * {@code buf.toString(buf.position(), buf.available()(), charsetName)}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public String toString(Charset charset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("ConcatenatedByteArrayInputStream{");
        builder.append("position=").append(previousBuffersSize + position);
        builder.append(", capacity=").append(totalSize);
        builder.append('}');
        return builder.toString();
    }
}



