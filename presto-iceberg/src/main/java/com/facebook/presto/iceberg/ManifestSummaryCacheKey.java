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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Identifies a folded manifest summary. Every input the summary is derived from is part of the key,
 * so an entry can never go stale and may be shared by any query, session or user:
 *
 * <ul>
 *   <li>the snapshot pins the set of data files, and a snapshot is immutable once written
 *   <li>the schema id pins the field-id to type mapping used to decode the per-file bounds
 *   <li>the partition spec id pins which fields the summary folds min/max over
 *   <li>the predicate and the requested columns determine which files are scanned and which
 *       columns' metrics are read
 * </ul>
 *
 * The table name is included for readability in cache dumps; the snapshot id alone already
 * distinguishes tables in practice.
 */
public class ManifestSummaryCacheKey
{
    private final SchemaTableName tableName;
    private final long snapshotId;
    private final int schemaId;
    private final int partitionSpecId;
    private final TupleDomain<IcebergColumnHandle> predicate;
    private final List<Integer> columnIds;

    public ManifestSummaryCacheKey(
            SchemaTableName tableName,
            long snapshotId,
            int schemaId,
            int partitionSpecId,
            TupleDomain<IcebergColumnHandle> predicate,
            List<Integer> columnIds)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.partitionSpecId = partitionSpecId;
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.columnIds = ImmutableList.copyOf(requireNonNull(columnIds, "columnIds is null"));
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ManifestSummaryCacheKey that = (ManifestSummaryCacheKey) other;
        return snapshotId == that.snapshotId &&
                schemaId == that.schemaId &&
                partitionSpecId == that.partitionSpecId &&
                tableName.equals(that.tableName) &&
                predicate.equals(that.predicate) &&
                columnIds.equals(that.columnIds);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableName, snapshotId, schemaId, partitionSpecId, predicate, columnIds);
    }

    @Override
    public String toString()
    {
        return "ManifestSummaryCacheKey{" +
                "tableName=" + tableName +
                ", snapshotId=" + snapshotId +
                ", schemaId=" + schemaId +
                ", partitionSpecId=" + partitionSpecId +
                ", columnIds=" + columnIds +
                '}';
    }
}
