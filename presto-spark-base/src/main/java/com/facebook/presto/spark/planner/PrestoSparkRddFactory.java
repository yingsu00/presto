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
package com.facebook.presto.spark.planner;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spark.PrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.IntegerIdentityPartitioner;
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spark.classloader_interface.SerializedPrestoSparkTaskDescriptor;
import com.facebook.presto.spark.classloader_interface.SerializedTaskStats;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SystemPartitioningHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.util.CollectionAccumulator;
import scala.Tuple2;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.getSparkInitialPartitionCount;
import static com.facebook.presto.spark.classloader_interface.TaskProcessors.createTaskProcessor;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.Collections.shuffle;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

public class PrestoSparkRddFactory
{
    private final SplitManager splitManager;
    private final Metadata metadata;
    private final JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec;

    @Inject
    public PrestoSparkRddFactory(SplitManager splitManager, Metadata metadata, JsonCodec<PrestoSparkTaskDescriptor> taskDescriptorJsonCodec)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.taskDescriptorJsonCodec = requireNonNull(taskDescriptorJsonCodec, "taskDescriptorJsonCodec is null");
    }

    public JavaPairRDD<Integer, PrestoSparkRow> createSparkRdd(
            JavaSparkContext sparkContext,
            Session session,
            PlanFragment fragment,
            Map<PlanFragmentId, JavaPairRDD<Integer, PrestoSparkRow>> rddInputs,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            TableWriteInfo tableWriteInfo)
    {
        checkArgument(!fragment.getStageExecutionDescriptor().isStageGroupedExecution(), "unexpected grouped execution fragment: %s", fragment.getId());

        PartitioningHandle partitioning = fragment.getPartitioning();

        if (!(partitioning.getConnectorHandle() instanceof SystemPartitioningHandle)) {
            // TODO: add support for bucketed table
            throw new PrestoException(NOT_SUPPORTED, "Partitioned (bucketed) tables are not yet supported by Presto on Spark");
        }

        if (partitioning.equals(SCALED_WRITER_DISTRIBUTION)) {
            throw new PrestoException(NOT_SUPPORTED, "Automatic writers scaling is not supported by Presto on Spark");
        }

        checkArgument(!partitioning.equals(COORDINATOR_DISTRIBUTION), "COORDINATOR_DISTRIBUTION fragment must be run on the driver");
        checkArgument(!partitioning.equals(FIXED_BROADCAST_DISTRIBUTION), "FIXED_BROADCAST_DISTRIBUTION can only be set as an output partitioning scheme, and not as a fragment distribution");
        checkArgument(!partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION), "FIXED_PASSTHROUGH_DISTRIBUTION can only be set as local exchange partitioning");

        // TODO: ARBITRARY_DISTRIBUTION is something very weird.
        // TODO: It doesn't have partitioning function, and it is never set as a fragment partitioning.
        // TODO: We should consider removing ARBITRARY_DISTRIBUTION.
        checkArgument(!partitioning.equals(ARBITRARY_DISTRIBUTION), "ARBITRARY_DISTRIBUTION is not expected to be set as a fragment distribution");

        int hashPartitionCount = getHashPartitionCount(session);

        // configure number of output partitions
        if (fragment.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION)) {
            fragment = fragment.withBucketToPartition(Optional.of(IntStream.range(0, hashPartitionCount).toArray()));
        }

        if (partitioning.equals(SINGLE_DISTRIBUTION) || partitioning.equals(FIXED_HASH_DISTRIBUTION) || partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            checkArgument(
                    fragment.getTableScanSchedulingOrder().isEmpty(),
                    "Fragment with is not expected to have table scans. fragmentId: %s, fragment partitioning %s",
                    fragment.getId(),
                    fragment.getPartitioning());

            for (RemoteSourceNode remoteSource : fragment.getRemoteSourceNodes()) {
                if (remoteSource.isEnsureSourceOrdering() || remoteSource.getOrderingScheme().isPresent()) {
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Order sensitive exchange is not supported by Presto on Spark. fragmentId: %s, sourceFragmentIds: %s",
                            fragment.getId(),
                            remoteSource.getSourceFragmentIds()));
                }
            }

            Partitioner inputPartitioner = createPartitioner(partitioning, hashPartitionCount);

            Map<PlanFragmentId, JavaPairRDD<Integer, PrestoSparkRow>> partitionedInputs = rddInputs.entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().partitionBy(inputPartitioner)));

            return createIntermediateRdd(
                    session,
                    fragment,
                    executorFactoryProvider,
                    taskStatsCollector,
                    tableWriteInfo,
                    partitionedInputs);
        }
        else if (partitioning.equals(SOURCE_DISTRIBUTION)) {
            checkArgument(rddInputs.isEmpty(), "rddInputs is expected to be empty for SOURCE_DISTRIBUTION fragment: %s", fragment.getId());
            return createSourceRdd(
                    sparkContext,
                    session,
                    fragment,
                    executorFactoryProvider,
                    taskStatsCollector,
                    tableWriteInfo);
        }
        else {
            throw new IllegalArgumentException(format("Unexpected fragment partitioning %s, fragmentId: %s", partitioning, fragment.getId()));
        }
    }

    private static Partitioner createPartitioner(PartitioningHandle partitioning, int partitionCount)
    {
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            return new IntegerIdentityPartitioner(1);
        }
        if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            return new IntegerIdentityPartitioner(partitionCount);
        }
        if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            throw new PrestoException(NOT_SUPPORTED, "FIXED_ARBITRARY_DISTRIBUTION partitioning is not yet supported");
        }
        throw new IllegalArgumentException(format("Unexpected fragment partitioning %s", partitioning));
    }

    private JavaPairRDD<Integer, PrestoSparkRow> createIntermediateRdd(
            Session session,
            PlanFragment fragment,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            TableWriteInfo tableWriteInfo,
            Map<PlanFragmentId, JavaPairRDD<Integer, PrestoSparkRow>> rddInputs)
    {
        List<TableScanNode> tableScans = findTableScanNodes(fragment.getRoot());
        verify(tableScans.isEmpty(), "no table scans is expected");

        Set<PlanFragmentId> expectedInputs = fragment.getRemoteSourceNodes().stream()
                .map(RemoteSourceNode::getSourceFragmentIds)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<PlanFragmentId> missingInputs = difference(expectedInputs, rddInputs.keySet());
        Set<PlanFragmentId> extraInputs = difference(rddInputs.keySet(), expectedInputs);
        checkArgument(
                missingInputs.isEmpty() && extraInputs.isEmpty(),
                "rddInputs mismatch discovered. expected: %s, actual: %s",
                expectedInputs,
                rddInputs.keySet());

        PrestoSparkTaskDescriptor taskDescriptor = createIntermediateTaskDescriptor(session, tableWriteInfo, fragment);
        SerializedPrestoSparkTaskDescriptor serializedTaskDescriptor = new SerializedPrestoSparkTaskDescriptor(taskDescriptorJsonCodec.toJsonBytes(taskDescriptor));

        if (rddInputs.size() == 1) {
            RemoteSourceNode remoteSourceNode = getOnlyElement(fragment.getRemoteSourceNodes());
            PairFlatMapFunction<Iterator<Tuple2<Integer, PrestoSparkRow>>, Integer, PrestoSparkRow> taskProcessor =
                    createTaskProcessor(
                            executorFactoryProvider,
                            serializedTaskDescriptor,
                            remoteSourceNode.getId().toString(),
                            taskStatsCollector);
            return getOnlyElement(rddInputs.values())
                    .mapPartitionsToPair(taskProcessor);
        }
        else if (rddInputs.size() == 2) {
            List<RemoteSourceNode> remoteSources = fragment.getRemoteSourceNodes();
            checkArgument(remoteSources.size() == 2, "two remote sources are expected, got: %s", remoteSources.size());
            RemoteSourceNode firstRemoteSource = remoteSources.get(0);
            RemoteSourceNode secondRemoteSource = remoteSources.get(1);
            JavaPairRDD<Integer, PrestoSparkRow> firstRdd = rddInputs.get(firstRemoteSource.getSourceFragmentIds().get(0));
            JavaPairRDD<Integer, PrestoSparkRow> secondRdd = rddInputs.get(secondRemoteSource.getSourceFragmentIds().get(0));
            FlatMapFunction2<Iterator<Tuple2<Integer, PrestoSparkRow>>, Iterator<Tuple2<Integer, PrestoSparkRow>>, Tuple2<Integer, PrestoSparkRow>> taskProcessor =
                    createTaskProcessor(
                            executorFactoryProvider,
                            serializedTaskDescriptor,
                            firstRemoteSource.getId().toString(),
                            secondRemoteSource.getId().toString(),
                            taskStatsCollector);
            return JavaPairRDD.fromJavaRDD(
                    firstRdd.zipPartitions(
                            secondRdd,
                            taskProcessor));
        }

        throw new IllegalArgumentException(format("unsupported number of inputs: %s", rddInputs.size()));
    }

    private JavaPairRDD<Integer, PrestoSparkRow> createSourceRdd(
            JavaSparkContext sparkContext,
            Session session,
            PlanFragment fragment,
            PrestoSparkTaskExecutorFactoryProvider executorFactoryProvider,
            CollectionAccumulator<SerializedTaskStats> taskStatsCollector,
            TableWriteInfo tableWriteInfo)
    {
        // TODO: Possible in case of a broadcast join
        checkArgument(fragment.getRemoteSourceNodes().isEmpty(), "source task with remote sources is not supported");

        List<TableScanNode> tableScans = findTableScanNodes(fragment.getRoot());
        checkArgument(
                tableScans.size() == 1,
                "exactly one table scan is expected in SOURCE_DISTRIBUTION fragment. fragmentId: %s, actual number of table scans: %s",
                fragment.getId(),
                tableScans.size());

        TableScanNode tableScan = getOnlyElement(tableScans);

        List<ScheduledSplit> splits = getSplits(session, tableScan);
        shuffle(splits);
        int initialPartitionCount = getSparkInitialPartitionCount(session);
        int numTasks = Math.min(splits.size(), initialPartitionCount);
        if (numTasks == 0) {
            return JavaPairRDD.fromJavaRDD(sparkContext.emptyRDD());
        }

        List<List<ScheduledSplit>> assignedSplits = assignSplitsToTasks(splits, numTasks);

        // let the garbage collector reclaim the memory used by the decoded splits as soon as the task descriptor is encoded
        splits = null;

        ImmutableList.Builder<SerializedPrestoSparkTaskDescriptor> serializedTaskDescriptors = ImmutableList.builder();
        for (int i = 0; i < assignedSplits.size(); i++) {
            List<ScheduledSplit> splitBatch = assignedSplits.get(i);
            PrestoSparkTaskDescriptor taskDescriptor = createSourceTaskDescriptor(session, tableWriteInfo, fragment, splitBatch);
            // TODO: consider more efficient serialization or apply compression to save precious memory on the Driver
            byte[] jsonSerializedTaskDescriptor = taskDescriptorJsonCodec.toJsonBytes(taskDescriptor);
            serializedTaskDescriptors.add(new SerializedPrestoSparkTaskDescriptor(jsonSerializedTaskDescriptor));
            // let the garbage collector reclaim the memory used by the decoded splits as soon as the task descriptor is encoded
            assignedSplits.set(i, null);
        }

        return sparkContext.parallelize(serializedTaskDescriptors.build(), numTasks)
                .mapPartitionsToPair(createTaskProcessor(executorFactoryProvider, taskStatsCollector));
    }

    private List<ScheduledSplit> getSplits(Session session, TableScanNode tableScan)
    {
        List<ScheduledSplit> splits = new ArrayList<>();
        SplitSource splitSource = splitManager.getSplits(session, tableScan.getTable(), UNGROUPED_SCHEDULING);
        long sequenceId = 0;
        while (!splitSource.isFinished()) {
            List<Split> splitBatch = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
            for (Split split : splitBatch) {
                splits.add(new ScheduledSplit(sequenceId++, tableScan.getId(), split));
            }
        }
        return splits;
    }

    private static List<List<ScheduledSplit>> assignSplitsToTasks(List<ScheduledSplit> splits, int numTasks)
    {
        checkArgument(numTasks > 0, "numTasks must be greater then zero");
        List<List<ScheduledSplit>> assignedSplits = new ArrayList<>();
        for (int i = 0; i < numTasks; i++) {
            assignedSplits.add(new ArrayList<>());
        }
        for (int splitIndex = 0; splitIndex < splits.size(); splitIndex++) {
            assignedSplits.get(splitIndex % numTasks).add(splits.get(splitIndex));
        }
        return assignedSplits;
    }

    private PrestoSparkTaskDescriptor createIntermediateTaskDescriptor(Session session, TableWriteInfo tableWriteInfo, PlanFragment fragment)
    {
        return createSourceTaskDescriptor(session, tableWriteInfo, fragment, ImmutableList.of());
    }

    private PrestoSparkTaskDescriptor createSourceTaskDescriptor(
            Session session,
            TableWriteInfo tableWriteInfo,
            PlanFragment fragment,
            List<ScheduledSplit> splits)
    {
        Map<PlanNodeId, Set<ScheduledSplit>> splitsByPlanNode = splits.stream()
                .collect(Collectors.groupingBy(
                        ScheduledSplit::getPlanNodeId,
                        mapping(identity(), toSet())));

        List<TaskSource> taskSourceByPlanNode = splitsByPlanNode.entrySet().stream()
                .map(entry -> new TaskSource(
                        entry.getKey(),
                        entry.getValue(),
                        ImmutableSet.of(),
                        true))
                .collect(toImmutableList());

        return new PrestoSparkTaskDescriptor(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                taskSourceByPlanNode,
                tableWriteInfo);
    }

    private static List<TableScanNode> findTableScanNodes(PlanNode node)
    {
        return searchFrom(node)
                .where(TableScanNode.class::isInstance)
                .findAll();
    }
}
