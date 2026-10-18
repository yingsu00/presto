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
package com.facebook.presto.nativeworker.iceberg;

import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;
import static java.util.Comparator.nullsLast;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Verifies that native workers sort the rows of each data file by the table's sort order.
public class TestPrestoNativeIcebergSortedWrites
        extends AbstractTestQueryFramework
{
    private static final long ROW_COUNT = 5_000;
    // Generates rows in ascending id order with nulls mixed in, so every sort order below has to reorder them.
    private static final String SOURCE_QUERY = "SELECT " +
            "IF(x % 10 = 0, NULL, x % 100) AS id, " +
            "IF(x % 7 = 0, NULL, CAST(x AS VARCHAR)) AS name, " +
            "CAST(x % 3 AS INTEGER) AS part " +
            "FROM UNNEST(sequence(1, 5000)) t(x)";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat("PARQUET")
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testInsertIntoSortedTable()
    {
        assertUpdate("DROP TABLE IF EXISTS test_sorted_insert");
        // DESC NULLS FIRST and ASC NULLS LAST are the two orders Hive sorting columns cannot express.
        assertUpdate("CREATE TABLE test_sorted_insert (id BIGINT, name VARCHAR, part INTEGER) " +
                "WITH (sorted_by = ARRAY['id DESC NULLS FIRST', 'name ASC NULLS LAST'])");
        assertUpdate("INSERT INTO test_sorted_insert " + SOURCE_QUERY, ROW_COUNT);

        assertQuery("SELECT count(*), count(id), count(name) FROM test_sorted_insert", "SELECT 5000, 4500, 4286");
        assertFilesSorted("test_sorted_insert", nullsFirst(Comparator.<Long>reverseOrder()), nullsLast(naturalOrder()));
    }

    @Test
    public void testCreateSortedTableAsSelect()
    {
        assertUpdate("DROP TABLE IF EXISTS test_sorted_ctas");
        assertUpdate("CREATE TABLE test_sorted_ctas WITH (sorted_by = ARRAY['id', 'name DESC']) AS " + SOURCE_QUERY, ROW_COUNT);

        // Iceberg places nulls first for ASC and last for DESC by default.
        assertFilesSorted("test_sorted_ctas", nullsFirst(naturalOrder()), nullsLast(Comparator.<String>reverseOrder()));
    }

    @Test
    public void testInsertIntoSortedPartitionedTable()
    {
        assertUpdate("DROP TABLE IF EXISTS test_sorted_partitioned");
        assertUpdate("CREATE TABLE test_sorted_partitioned (id BIGINT, name VARCHAR, part INTEGER) " +
                "WITH (partitioning = ARRAY['part'], sorted_by = ARRAY['id DESC', 'name'])");
        assertUpdate("INSERT INTO test_sorted_partitioned " + SOURCE_QUERY, ROW_COUNT);

        List<String> files = getDataFiles("test_sorted_partitioned");
        assertTrue(files.size() >= 3, format("Expected a file per partition, got %s", files));
        assertFilesSorted("test_sorted_partitioned", nullsLast(Comparator.<Long>reverseOrder()), nullsFirst(naturalOrder()));
    }

    private void assertFilesSorted(String table, Comparator<Long> idOrder, Comparator<String> nameOrder)
    {
        Comparator<Group> rowOrder = Comparator.comparing(row -> getValue(row, "id", group -> group.getLong("id", 0)), idOrder);
        rowOrder = rowOrder.thenComparing(row -> getValue(row, "name", group -> group.getString("name", 0)), nameOrder);

        long rowCount = 0;
        for (String file : getDataFiles(table)) {
            List<Group> rows = readRows(file);
            for (int i = 1; i < rows.size(); i++) {
                assertTrue(rowOrder.compare(rows.get(i - 1), rows.get(i)) <= 0,
                        format("Rows %s and %s of %s are out of order: %s, %s", i - 1, i, file, rows.get(i - 1), rows.get(i)));
            }
            rowCount += rows.size();
        }
        assertEquals(rowCount, ROW_COUNT);
    }

    private static <T> T getValue(Group row, String column, Function<Group, T> reader)
    {
        return row.getFieldRepetitionCount(column) == 0 ? null : reader.apply(row);
    }

    private List<String> getDataFiles(String table)
    {
        return computeActual(format("SELECT file_path FROM \"%s$files\"", table)).getMaterializedRows().stream()
                .map(row -> (String) row.getField(0))
                .collect(toImmutableList());
    }

    /// Reads the rows of a Parquet file in the order they are stored.
    private static List<Group> readRows(String file)
    {
        ImmutableList.Builder<Group> rows = ImmutableList.builder();
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(file))
                .withConf(new Configuration())
                .build()) {
            for (Group row = reader.read(); row != null; row = reader.read()) {
                rows.add(row);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rows.build();
    }
}
