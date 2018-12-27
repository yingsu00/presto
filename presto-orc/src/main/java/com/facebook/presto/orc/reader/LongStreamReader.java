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
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class LongStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final LongDirectStreamReader directReader;
    private final LongDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public LongStreamReader(StreamDescriptor streamDescriptor, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        directReader = new LongDirectStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(LongStreamReader.class.getSimpleName()));
        dictionaryReader = new LongDictionaryStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(LongStreamReader.class.getSimpleName()));
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        currentReader.prepareNextRead(batchSize);
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        return currentReader.readBlock(type);
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind kind = encoding.get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getColumnEncodingKind();
        if (kind == DIRECT || kind == DIRECT_V2 || kind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (kind == DICTIONARY) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        currentReader.setInputQualifyingSet(qualifyingSet);
    }

    @Override
    public QualifyingSet getInputQualifyingSet()
    {
        return currentReader.getInputQualifyingSet();
    }

    @Override
    public QualifyingSet getOutputQualifyingSet()
    {
        return currentReader.getOutputQualifyingSet();
    }

    @Override
    public void setFilterAndChannel(Filter filter, int channel, int columnIndex)
    {
        directReader.setFilterAndChannel(filter, channel, columnIndex);
    }

    @Override
    public int getChannel()
    {
        return directReader.getChannel();
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        return currentReader.getBlock(numFirstRows, mayReuse);
    }

    @Override
    public Filter getFilter()
    {
        return directReader.getFilter();
    }

    @Override
    public int getFixedWidth()
    {
        return SIZE_OF_LONG;
    }

    @Override
    public void erase(int end)
    {
        if (currentReader == null) {
            return;
        }
        currentReader.erase(end);
    }

    @Override
    public void compactValues(int[] positions, int base, int numPositions)
    {
        currentReader.compactValues(positions, base, numPositions);
    }

    @Override
    public int getTruncationRow()
    {
        return currentReader.getTruncationRow();
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (currentReader == null) {
            return 0;
        }
        return currentReader.getResultSizeInBytes();
    }

    @Override
    public void setResultSizeBudget(int bytes)
    {
        currentReader.setResultSizeBudget(bytes);
    }

    public void scan()
            throws IOException
    {
        currentReader.scan();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> directReader.close());
            closer.register(() -> dictionaryReader.close());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + directReader.getRetainedSizeInBytes() + dictionaryReader.getRetainedSizeInBytes();
    }
}
