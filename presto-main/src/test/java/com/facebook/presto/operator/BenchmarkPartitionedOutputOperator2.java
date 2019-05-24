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
package com.facebook.presto.operator;

import com.facebook.presto.Session;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.client.ClientCapabilities;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.PartitionedOutputOperator.PartitionedOutputFactory;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LongArrayBlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingTaskContext;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.lang.invoke.MethodHandle;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZED_PARTITIONED_OUTPUT;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.Arrays.stream;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(0)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperator2
{
    @Benchmark
    public void addPage(BenchmarkData data)
    {
        PartitionedOutputOperator operator = data.createPartitionedOutputOperator();
        for (int i = 0; i < data.PAGE_COUNT; i++) {
            operator.addInput(data.dataPage);
        }
        operator.finish();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"true", "false"})
        private String useOptimized = "false";

        @Param({"1", "2"})
        //@Param({"1"})
        private int channelCount = 1;

        //@Param({"512", "2048", "8192", "32768"})
        @Param({"8192"})
        private int positionCount = 8192;

        // @Param({"BIGINT", "BOOLEAN", "DOUBLE", "ROW(VARCHAR)", "ARRAY(BIGINT)", "MAP(BIGINT, VARCHAR)", "DICTIONARY", "RLE"})
        @Param({"BIGINT"})
        private String type = "BIGINT";

        //@Param({"true", "false"})
        private boolean hasNull = true;

        private static final int PARTITION_COUNT = 512;
        private static final int PAGE_COUNT = 5000;
        private static final int ROW_SIZE = 4;
        private static final int ARRAY_SIZE = 10;
        private static final int MAP_SIZE = 10;
        private static final int STRING_SIZE = 10;

        private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);

        private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
        private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));
        private static final Random random = new Random();

        private Page dataPage;
        private List<Type> types;

        @Setup
        public void setup()
        {
            createPages(type);
        }

        private void createPages(String inputType)
        {
            switch (inputType) {
                case "BIGINT":
                    createPagesWithBuilders(this::createBigintChannel, BIGINT);
                    break;
                case "BOOLEAN":
                    createPagesWithBuilders(this::createBooleanChannel, BOOLEAN);
                    break;
                case "DOUBLE":
                    createPagesWithBuilders(this::createDoubleChannel, DOUBLE);
                    break;
                case "VARCHAR":
                    createPagesWithBuilders(this::createVarcharChannel, VARCHAR);
                    break;
                case "ROW(VARCHAR)":
                    createPagesWithBuilders(this::createRowChannel, RowType.anonymous(nCopies(ROW_SIZE, VARCHAR)));
                    break;
                case "ARRAY(BIGINT)":
                    createPagesWithBuilders(this::createArrayChannel, new ArrayType(BIGINT));
                    break;
                case "MAP(BIGINT, VARCHAR)":
                    Type keyType = BIGINT;
                    Type valueType = VARCHAR;
                    MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
                    MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
                    MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
                    MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
                    MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
                    Type mapType = new MapType(
                            keyType,
                            valueType,
                            keyBlockNativeEquals,
                            keyBlockEquals,
                            keyNativeHashCode,
                            keyBlockHashCode);
                    createPagesWithBuilders(this::createMapChannel, mapType);
                    break;
                case "DICTIONARY":
                    createDictionaryPages();
                    break;
                case "RLE":
                    createRLEPages();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType");
            }
        }

        private void createPagesWithBuilders(BiConsumer<BlockBuilder, Type> createChannelConsumer, Type type)
        {
            types = new ArrayList<>(channelCount + 1);
            types.add(0, BIGINT);
            for (int i = 1; i <= channelCount; i++) {
                types.add(i, type);
            }

            PageBuilder pageBuilder = new PageBuilder(types);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            createBigintChannel(blockBuilder, BIGINT);

            for (int channelIndex = 1; channelIndex <= channelCount; channelIndex++) {
                blockBuilder = pageBuilder.getBlockBuilder(channelIndex);
                createChannelConsumer.accept(blockBuilder, type);
            }
            pageBuilder.declarePositions(positionCount);

            dataPage = pageBuilder.build();
        }


        private void createDictionaryPages()
        {
            Block[] blocks = new Block[channelCount + 1];
            blocks[0] = createBigintChannel();
            for (int channelIndex = 1; channelIndex < channelCount + 1; channelIndex++) {
                blocks[channelIndex] = createDictionaryChannel();
            }
            dataPage = new Page(blocks);
        }

        private void createRLEPages()
        {
            Block[] blocks = new Block[channelCount + 1];
            blocks[0] = createBigintChannel();
            for (int channelIndex = 1; channelIndex < channelCount + 1; channelIndex++) {
                blocks[channelIndex] = createRLEChannel();
            }
            dataPage = new Page(blocks);
        }

        private Block createBigintChannel()
        {
            LongArrayBlockBuilder builder = new LongArrayBlockBuilder(null, positionCount);
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    builder.appendNull();
                }
                else {
                    BIGINT.writeLong(builder, ThreadLocalRandom.current().nextLong());
                }
            }
            return builder.build();
        }
//
//        private Block createVarcharChannel()
//        {
//            VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, positionCount, positionCount * STRING_SIZE);
//            for (int position = 0; position < positionCount; position++) {
//                if (hasNull && position % 10 == 0) {
//                    blockBuilder.appendNull();
//                }
//                else {
//                    createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(getRandomString(ThreadLocalRandom.current().nextInt(100))));
//                }
//            }
//            return blockBuilder.build();
//        }

        private void createBigintChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeLong(blockBuilder, position);
                }
            }
        }

        private void createBooleanChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeBoolean(blockBuilder, ThreadLocalRandom.current().nextBoolean());
                }
            }
        }

        private void createDoubleChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeDouble(blockBuilder, position);
                }
            }
        }

        private void createVarcharChannel(BlockBuilder blockBuilder, Type type)
        {
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    type.writeSlice(blockBuilder, Slices.utf8Slice(getRandomString(ThreadLocalRandom.current().nextInt(100))));
                }
            }
        }

        private void createRowChannel(BlockBuilder blockBuilder, Type type)
        {
            List<Type> subTypes = ((RowType) type).getTypeParameters();
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder singleRowBlockWriter = blockBuilder.beginBlockEntry();
                    for (Type subType : subTypes) {
                        subType.writeSlice(singleRowBlockWriter, utf8Slice(getRandomString(10)));
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createArrayChannel(BlockBuilder blockBuilder, Type type)
        {
            ArrayType arrayType = (ArrayType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < ARRAY_SIZE; i++) {
                        arrayType.getElementType().writeLong(entryBuilder, ThreadLocalRandom.current().nextLong());
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private void createMapChannel(BlockBuilder blockBuilder, Type type)
        {
            MapType mapType = (MapType) type;
            for (int position = 0; position < positionCount; position++) {
                if (hasNull && position % 10 == 0) {
                    blockBuilder.appendNull();
                }
                else {
                    BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
                    for (int i = 0; i < MAP_SIZE; i++) {
                        mapType.getKeyType().writeLong(entryBuilder, position + i);
                        mapType.getValueType().writeSlice(entryBuilder, Slices.utf8Slice(getRandomString(ThreadLocalRandom.current().nextInt(100))));
                    }
                    blockBuilder.closeEntry();
                }
            }
        }

        private Block createDictionaryChannel()
        {
            types = nCopies(channelCount, VARBINARY);
            BlockBuilder dictionaryBuilder = VARBINARY.createBlockBuilder(null, 100);
            for (int position = 0; position < positionCount; position++) {
                VARBINARY.writeSlice(dictionaryBuilder, utf8Slice("testString" + position));
            }
            Block dictionary = dictionaryBuilder.build();

            int[] ids = new int[positionCount];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = i;
            }
            return new DictionaryBlock(dictionary, ids);
        }

        private Block createRLEChannel()
        {
            types = nCopies(channelCount, VARCHAR);
            return RunLengthEncodedBlock.create(VARCHAR, utf8Slice(getRandomString(100)), positionCount);
        }

        private static String getRandomString(int size)
        {
            byte[] array = new byte[size];
            ThreadLocalRandom.current().nextBytes(array);
            return new String(array, Charset.forName("UTF-8"));
        }

        private PartitionedOutputOperator createPartitionedOutputOperator()
        {
            PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);

            //PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
            PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(new TypeRegistry()), false);
            OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
            for (int partition = 0; partition < PARTITION_COUNT; partition++) {
                buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
            }
            PartitionedOutputBuffer buffer = createPartitionedBuffer(
                    buffers.withNoMoreBufferIds(),
                    new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
            buffer.registerLifespanCompletionCallback(ignore -> {});
            PartitionedOutputFactory operatorFactory = new PartitionedOutputFactory(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    false,
                    OptionalInt.empty(),
                    buffer,
                    new DataSize(1, GIGABYTE));
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), serdeFactory)
                    .createOperator(createDriverContext());
        }

        private DriverContext createDriverContext()
        {
            Session testSession = testSessionBuilder()
                    .setCatalog("tpch")
                    .setSchema(TINY_SCHEMA_NAME)
                    .setClientCapabilities(stream(ClientCapabilities.values())
                            .map(ClientCapabilities::toString)
                            .collect(toImmutableSet()))
                    .setSystemProperty(OPTIMIZED_PARTITIONED_OUTPUT, useOptimized)
                    .build();

            return TestingTaskContext.builder(EXECUTOR, SCHEDULER, testSession)
                    .setMemoryPoolSize(MAX_MEMORY)
                    .build()
                    .addPipelineContext(0, true, true, false)
                    .addDriverContext();
        }

        private PartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
        {
            return new PartitionedOutputBuffer(
                    "task-instance-id",
                    new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                    buffers,
                    dataSize,
                    () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                    SCHEDULER);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkPartitionedOutputOperator2().addPage(data);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx50g")
                .include(".*" + BenchmarkPartitionedOutputOperator2.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
