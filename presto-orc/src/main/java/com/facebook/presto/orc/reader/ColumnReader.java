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

import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.spi.block.Block;

abstract class ColumnReader
        implements StreamReader
{
    QualifyingSet inputQualifyingSet;
    QualifyingSet outputQualifyingSet;
    Block block;
    int outputChannel = -1;
    Filter filter;
    int columnIndex;
    int expectNumValues = 10000;
    // First row number in row group that is not processed due to
    // reaching target size. This must occur as a position in
    // inputQualifyingSet. -1 if all inputQualifyingSet is processed.
    int truncationRow = -1;

    // Number of values in Block to be returned by getBlock.
    int numValues;

    // Number of bytes the next scan() may add to the result.
    int resultSizeBudget = 8 * 10000;

    public QualifyingSet getInputQualifyingSet()
    {
        return inputQualifyingSet;
    }

    public void setInputQualifyingSet(QualifyingSet qualifyingSet)
    {
        inputQualifyingSet = qualifyingSet;
    }

    public QualifyingSet getOutputQualifyingSet()
    {
        return outputQualifyingSet;
    }

    @Override
    public void setFilterAndChannel(Filter filter, int channel, int columnIndex)
    {
        this.filter = filter;
        outputChannel = channel;
        this.columnIndex = columnIndex;
    }

    @Override
    public int getChannel()
    {
        return outputChannel;
    }
    @Override
    public Filter getFilter()
    {
        return filter;
    }

    @Override
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @Override
    public void setResultSizeBudget(int bytes)
    {
        resultSizeBudget = bytes;
    }

    @Override
    public int getTruncationRow()
    {
        return truncationRow;
    }

    @Override
    public int getResultSizeInBytes()
    {
        int fixedSize = getFixedWidth();
        if (fixedSize != -1) {
            return numValues * fixedSize;
        }
        throw new UnsupportedOperationException("Variable width streams must implement getResultSizeInBytes()");
    }

    public void compactQualifyingSet(int[] surviving, int numSurviving)
    {
        if (outputQualifyingSet == null) {
            return;
        }
        int[] rows = outputQualifyingSet.getMutablePositions(0);
        for (int i = 0; i < numSurviving; i++) {
            rows[i] = rows[surviving[i]];
        }
        outputQualifyingSet.setPositionCount(numSurviving);
    }
}
