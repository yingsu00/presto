package com.facebook.presto.spi.block;

public interface UncheckedBlock
    extends Block
{
    default long getLongUnchecked(int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
