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
package com.facebook.presto.operator.project;

import com.facebook.presto.array.ReferenceCountMap;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.DictionaryId;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.sql.gen.ExpressionProfiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.SizeOf;

import javax.annotation.concurrent.NotThreadSafe;

import static java.lang.Math.max;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.HashSet;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.facebook.presto.operator.project.PageProcessorOutput.EMPTY_PAGE_PROCESSOR_OUTPUT;
import static com.facebook.presto.operator.project.SelectedPositions.positionsRange;
import static com.facebook.presto.spi.block.DictionaryId.randomDictionaryId;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.singletonIterator;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PageProcessor
{
    public static final int MAX_BATCH_SIZE = 8 * 1024;
    static final int MAX_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;
    static final int MIN_PAGE_SIZE_IN_BYTES = 1024 * 1024;

    private final ExpressionProfiler expressionProfiler;
    private final DictionarySourceIdFunction dictionarySourceIdFunction = new DictionarySourceIdFunction();
    private final Optional<PageFilter> filter;
    private final Optional<PageFilter> filterWithoutTupleDomain;
    private final List<PageProjection> projections;
    private final int[] inputToOutputChannel;
    boolean filterPushedDown = false;
    private int projectBatchSize;
    
    @VisibleForTesting
    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections, OptionalInt initialBatchSize)
    {
        this(filter, projections, initialBatchSize, new ExpressionProfiler());
    }

    @VisibleForTesting
    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections, OptionalInt initialBatchSize, ExpressionProfiler expressionProfiler)
    {
        this(filter, Optional.empty(), projections, initialBatchSize, expressionProfiler);
    }

    public PageProcessor(Optional<PageFilter> filter, Optional<PageFilter> filterWithoutTupleDomain, List<? extends PageProjection> projections, OptionalInt initialBatchSize)
    {
        this(filter, filterWithoutTupleDomain, projections, initialBatchSize, new ExpressionProfiler());
    }
    
    public PageProcessor(Optional<PageFilter> filter, Optional<PageFilter> filterWithoutTupleDomain, List<? extends PageProjection> projections, OptionalInt initialBatchSize, ExpressionProfiler expressionProfiler)
    {
        this.filter = requireNonNull(filter, "filter is null")
        .map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return new DictionaryAwarePageFilter(pageFilter);
                    }
                    return pageFilter;
                });
        this.filterWithoutTupleDomain = requireNonNull(filterWithoutTupleDomain, "filterWithoutTupleDomain is null");
        this.projections = requireNonNull(projections, "projections is null").stream()
                .map(projection -> {
                    if (projection.getInputChannels().size() == 1 && projection.isDeterministic()) {
                        return new DictionaryAwarePageProjection(projection, dictionarySourceIdFunction);
                    }
                    return projection;
                })
                .collect(toImmutableList());
        HashMap<Integer, Integer> inputToOutput = new HashMap();
        // If the projection only reorders columns, set
        // inputToOutputChannel[i] to give the output channel of the
        // ith channel in the output Page of the PageSource. If the
        // ith channel is not projected out, use -1.
        boolean allAreInputPageProjections = true;
        int maxInputChannel = -1;
        for (int i = 0; i < projections.size(); i++) {
            PageProjection projection = projections.get(i);
            if (!(projection instanceof InputPageProjection)) {
                allAreInputPageProjections = false;
                break;
            }
            List<Integer> inputs = projection.getInputChannels().getInputChannels();
            if (!inputToOutput.containsKey(inputs.get(0))) {
                inputToOutput.put(inputs.get(0), new Integer(i));
                maxInputChannel = Math.max(maxInputChannel, inputs.get(0).intValue());
            } else {
                allAreInputPageProjections = false;
                break;
            }
        }
        if (allAreInputPageProjections) {
            inputToOutputChannel = new int[maxInputChannel + 1];
            Arrays.fill(inputToOutputChannel, -1);
            for (Map.Entry<Integer, Integer> entry : inputToOutput.entrySet()) {
                inputToOutputChannel[entry.getKey().intValue()] = entry.getValue().intValue();
            }
        }
        else {
            inputToOutputChannel = null;
        }
        this.projectBatchSize = initialBatchSize.orElse(1);
        this.expressionProfiler = requireNonNull(expressionProfiler, "expressionProfiler is null");
    }

    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections)
    {
        this(filter, projections, OptionalInt.of(1));
    }

    public Optional<PageFilter> getFilterWithoutTupleDomain()
    {
        return filterWithoutTupleDomain;
    }

    public void setFilterIsPushedDown()
    {
        filterPushedDown = true;
    }
    
    public PageProcessorOutput process(ConnectorSession session, DriverYieldSignal yieldSignal, Page page)
    {
        // limit the scope of the dictionary ids to just one page
        dictionarySourceIdFunction.reset();

        if (page.getPositionCount() == 0) {
            return EMPTY_PAGE_PROCESSOR_OUTPUT;
        }

        if (!filterPushedDown && filter.isPresent()) {
            SelectedPositions selectedPositions = filter.get().filter(session, filter.get().getInputChannels().getInputChannels(page));
            if (selectedPositions.isEmpty()) {
                return EMPTY_PAGE_PROCESSOR_OUTPUT;
            }

            if (projections.isEmpty()) {
                return new PageProcessorOutput(() -> calculateRetainedSizeWithoutLoading(page), singletonIterator(Optional.of(new Page(selectedPositions.size()))));
            }

            if (selectedPositions.size() != page.getPositionCount()) {
                PositionsPageProcessorIterator pages = new PositionsPageProcessorIterator(session, yieldSignal, page, selectedPositions);
                return new PageProcessorOutput(pages::getRetainedSizeInBytes, pages);
            }
        }

        PositionsPageProcessorIterator pages = new PositionsPageProcessorIterator(session, yieldSignal, page, positionsRange(0, page.getPositionCount()));
        return new PageProcessorOutput(pages::getRetainedSizeInBytes, pages);
    }

    // Returns the channel numbers of the PageSource in the order they
    // are in the result Page. If this is non-null the projection is
    // pushed down into the PageSource.
    public int[] getIdentityInputToOutputChannel()
    {
        return inputToOutputChannel;
    }

    // Returns the channels of the PageSource that need to have a
    // Block for use in projection. Channels that only serve for
    // pushed down filters are not included.
    public int[] getOutputChannels()
    {
        HashSet<Integer> channels = new HashSet();
        for (PageProjection projection : projections) {
            for (Integer channel : projection.getInputChannels().getInputChannels()) {
                channels.add(channel);
            }
        }
        return channels.stream().mapToInt(Integer::intValue).toArray();
    }

    @VisibleForTesting
    public List<PageProjection> getProjections()
    {
        return projections;
    }

    private static boolean isUnloadedLazyBlock(Block block)
    {
        return (block instanceof LazyBlock) && !((LazyBlock) block).isLoaded();
    }

    private static long calculateRetainedSizeWithoutLoading(Page page)
    {
        long retainedSizeInBytes = Page.INSTANCE_SIZE + SizeOf.sizeOfObjectArray(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (!isUnloadedLazyBlock(block)) {
                retainedSizeInBytes += block.getRetainedSizeInBytes();
            }
        }
        return retainedSizeInBytes;
    }

    private class PositionsPageProcessorIterator
            extends AbstractIterator<Optional<Page>>
    {
        private final ConnectorSession session;
        private final DriverYieldSignal yieldSignal;
        private final Page page;

        private SelectedPositions selectedPositions;
        private final Block[] previouslyComputedResults;
        private long retainedSizeInBytes;

        // remember if we need to re-use the same batch size if we yield last time
        private boolean lastComputeYielded;
        private int lastComputeBatchSize;
        private Work<Block> pageProjectWork;

        public PositionsPageProcessorIterator(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
        {
            this.session = session;
            this.yieldSignal = yieldSignal;
            this.page = page;
            this.selectedPositions = selectedPositions;
            this.previouslyComputedResults = new Block[projections.size()];
            updateRetainedSize();
        }

        public long getRetainedSizeInBytes()
        {
            return retainedSizeInBytes;
        }

        @Override
        protected Optional<Page> computeNext()
        {
            int batchSize;
            while (true) {
                if (selectedPositions.isEmpty()) {
                    updateRetainedSize();
                    verify(!lastComputeYielded);
                    return endOfData();
                }

                // we always process one chunk
                if (lastComputeYielded) {
                    // re-use the batch size from the last checkpoint
                    verify(lastComputeBatchSize > 0);
                    batchSize = lastComputeBatchSize;
                    lastComputeYielded = false;
                    lastComputeBatchSize = 0;
                }
                else {
                    batchSize = Math.min(selectedPositions.size(), projectBatchSize);
                }
                ProcessBatchResult result = processBatch(batchSize);

                if (result.isYieldFinish()) {
                    // if we are running out of time, save the batch size and continue next time
                    lastComputeYielded = true;
                    lastComputeBatchSize = batchSize;
                    return Optional.empty();
                }

                if (result.isPageTooLarge()) {
                    // if the page buffer filled up, so halve the batch size and retry
                    verify(batchSize > 1);
                    projectBatchSize = projectBatchSize / 2;
                    continue;
                }

                verify(result.isSuccess());
                Page page = result.getPage();

                // if we produced a large page or if the expression is expensive, halve the batch size for the next call
                long pageSize = page.getSizeInBytes();
                if (page.getPositionCount() > 1 && (pageSize > MAX_PAGE_SIZE_IN_BYTES || expressionProfiler.isExpressionExpensive())) {
                    projectBatchSize = projectBatchSize / 2;
                }

                // if we produced a small page, double the batch size for the next call
                if (pageSize < MIN_PAGE_SIZE_IN_BYTES && projectBatchSize < MAX_BATCH_SIZE && !expressionProfiler.isExpressionExpensive()) {
                    projectBatchSize = projectBatchSize * 2;
                }

                // remove batch from selectedPositions and previouslyComputedResults
                selectedPositions = selectedPositions.subRange(batchSize, selectedPositions.size());
                for (int i = 0; i < previouslyComputedResults.length; i++) {
                    if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() > batchSize) {
                        previouslyComputedResults[i] = previouslyComputedResults[i].getRegion(batchSize, previouslyComputedResults[i].getPositionCount() - batchSize);
                    }
                    else {
                        previouslyComputedResults[i] = null;
                    }
                }

                updateRetainedSize();
                return Optional.of(page);
            }
        }

        private void updateRetainedSize()
        {
            // increment the size only when it is the first reference
            retainedSizeInBytes = Page.INSTANCE_SIZE + SizeOf.sizeOfObjectArray(page.getChannelCount());
            ReferenceCountMap referenceCountMap = new ReferenceCountMap();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                if (!isUnloadedLazyBlock(block)) {
                    block.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }
            for (Block previouslyComputedResult : previouslyComputedResults) {
                if (previouslyComputedResult != null) {
                    previouslyComputedResult.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }
        }

        private ProcessBatchResult processBatch(int batchSize)
        {
            Block[] blocks = new Block[projections.size()];

            int pageSize = 0;
            SelectedPositions positionsBatch = selectedPositions.subRange(0, batchSize);
            for (int i = 0; i < projections.size(); i++) {
                if (yieldSignal.isSet()) {
                    return ProcessBatchResult.processBatchYield();
                }

                if (positionsBatch.size() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    return ProcessBatchResult.processBatchTooLarge();
                }

                // if possible, use previouslyComputedResults produced in prior optimistic failure attempt
                PageProjection projection = projections.get(i);
                if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() >= batchSize) {
                    blocks[i] = previouslyComputedResults[i].getRegion(0, batchSize);
                }
                else {
                    if (pageProjectWork == null) {
                        expressionProfiler.start();
                        pageProjectWork = projection.project(session, yieldSignal, projection.getInputChannels().getInputChannels(page), positionsBatch);
                        expressionProfiler.stop(positionsBatch.size());
                    }
                    if (!pageProjectWork.process()) {
                        return ProcessBatchResult.processBatchYield();
                    }
                    previouslyComputedResults[i] = pageProjectWork.getResult();
                    pageProjectWork = null;
                    blocks[i] = previouslyComputedResults[i];
                }

                pageSize += blocks[i].getSizeInBytes();
            }
            return ProcessBatchResult.processBatchSuccess(new Page(positionsBatch.size(), blocks));
        }
    }

    @NotThreadSafe
    private static class DictionarySourceIdFunction
            implements Function<DictionaryBlock, DictionaryId>
    {
        private final Map<DictionaryId, DictionaryId> dictionarySourceIds = new HashMap<>();

        @Override
        public DictionaryId apply(DictionaryBlock block)
        {
            return dictionarySourceIds.computeIfAbsent(block.getDictionarySourceId(), ignored -> randomDictionaryId());
        }

        public void reset()
        {
            dictionarySourceIds.clear();
        }
    }

    private static class ProcessBatchResult
    {
        private final ProcessBatchState state;
        private final Page page;

        private ProcessBatchResult(ProcessBatchState state, Page page)
        {
            this.state = state;
            this.page = page;
        }

        public static ProcessBatchResult processBatchYield()
        {
            return new ProcessBatchResult(ProcessBatchState.YIELD, null);
        }

        public static ProcessBatchResult processBatchTooLarge()
        {
            return new ProcessBatchResult(ProcessBatchState.PAGE_TOO_LARGE, null);
        }

        public static ProcessBatchResult processBatchSuccess(Page page)
        {
            return new ProcessBatchResult(ProcessBatchState.SUCCESS, requireNonNull(page));
        }

        public boolean isYieldFinish()
        {
            return state == ProcessBatchState.YIELD;
        }

        public boolean isPageTooLarge()
        {
            return state == ProcessBatchState.PAGE_TOO_LARGE;
        }

        public boolean isSuccess()
        {
            return state == ProcessBatchState.SUCCESS;
        }

        public Page getPage()
        {
            verify(page != null);
            verify(state == ProcessBatchState.SUCCESS);
            return page;
        }

        private enum ProcessBatchState
        {
            YIELD,
            PAGE_TOO_LARGE,
            SUCCESS
        }
    }
}
