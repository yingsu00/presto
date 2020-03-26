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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.page.PageCompressor;
import com.facebook.presto.spi.page.PageDecompressor;
import com.facebook.presto.spi.spiller.SpillCipher;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import java.nio.ByteBuffer;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final boolean compressionEnabled;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionEnabled = compressionEnabled;
    }

    public PagesSerde createPagesSerde()
    {
        return createPagesSerdeInternal(Optional.empty());
    }

    public PagesSerde createPagesSerdeForSpill(Optional<SpillCipher> spillCipher)
    {
        return createPagesSerdeInternal(spillCipher);
    }

    private PagesSerde createPagesSerdeInternal(Optional<SpillCipher> spillCipher)
    {
        if (compressionEnabled) {
            return new PagesSerde(
                    blockEncodingSerde,
                    Optional.of(new PageCompressor()
                    {
                        Compressor compressor = new Lz4Compressor();
                        @Override
                        public int maxCompressedLength(int uncompressedSize)
                        {
                            return compressor.maxCompressedLength(uncompressedSize);
                        }

                        @Override
                        public int compress(
                                byte[] input,
                                int inputOffset,
                                int inputLength,
                                byte[] output,
                                int outputOffset,
                                int maxOutputLength)
                        {
                            return compressor.compress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
                        }

                        @Override
                        public void compress(ByteBuffer input, ByteBuffer output)
                        {
                            compressor.compress(input, output);
                        }
                    }),
                    Optional.of(new PageDecompressor()
                    {
                        Decompressor decompressor = new Lz4Decompressor();
                        @Override
                        public int decompress(
                                byte[] input,
                                int inputOffset,
                                int inputLength,
                                byte[] output,
                                int outputOffset,
                                int maxOutputLength)
                        {
                            return decompressor.decompress(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
                        }

                        @Override
                        public void decompress(ByteBuffer input, ByteBuffer output)
                        {
                            decompressor.decompress(input, output);
                        }
                    }),
                    spillCipher);
        }

        return new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), spillCipher);
    }
}
