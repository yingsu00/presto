package com.facebook.presto.spi.block;


public interface ByteArrayAllocator
{
    byte[]  allocate(int size);
    void free(byte[] bytes);
}

