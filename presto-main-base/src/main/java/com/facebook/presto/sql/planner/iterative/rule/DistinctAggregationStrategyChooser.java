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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.distinctAggregationsStrategy;
import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.SINGLE_STEP;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.SPLIT_TO_SUBQUERIES;
import static com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct.canUseMarkDistinct;
import static com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationsToSubqueries.isAggregationCandidateForSplittingToSubqueries;
import static com.facebook.presto.sql.planner.iterative.rule.PreAggregateDistinctAggregations.canUsePreAggregate;
import static com.facebook.presto.sql.planner.iterative.rule.PreAggregateDistinctAggregations.distinctAggregationsUniqueArgumentCount;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * Chooses the distinct aggregation implementation for a particular aggregation node.
 */
public class DistinctAggregationStrategyChooser
{
    private static final int MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = 8;
    private static final int PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER = MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * 8;
    private static final double MAX_JOIN_GROUPING_KEYS_SIZE = 100 * 1024 * 1024;

    private final TaskCountEstimator taskCountEstimator;

    public DistinctAggregationStrategyChooser(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    public static DistinctAggregationStrategyChooser createDistinctAggregationStrategyChooser(TaskCountEstimator taskCountEstimator)
    {
        return new DistinctAggregationStrategyChooser(taskCountEstimator);
    }

    public boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        return chooseMarkDistinctStrategy(aggregationNode, session, statsProvider, lookup) == MARK_DISTINCT;
    }

    public boolean shouldUsePreAggregate(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        return chooseMarkDistinctStrategy(aggregationNode, session, statsProvider, lookup) == PRE_AGGREGATE;
    }

    public boolean shouldSplitToSubqueries(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        return chooseMarkDistinctStrategy(aggregationNode, session, statsProvider, lookup) == SPLIT_TO_SUBQUERIES;
    }

    private DistinctAggregationsStrategy chooseMarkDistinctStrategy(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(session);
        if (distinctAggregationsStrategy != AUTOMATIC) {
            if (distinctAggregationsStrategy == MARK_DISTINCT && canUseMarkDistinct(aggregationNode)) {
                return MARK_DISTINCT;
            }
            if (distinctAggregationsStrategy == PRE_AGGREGATE && canUsePreAggregate(aggregationNode)) {
                return PRE_AGGREGATE;
            }
            if (distinctAggregationsStrategy == SPLIT_TO_SUBQUERIES && isAggregationCandidateForSplittingToSubqueries(aggregationNode) && isAggregationSourceSupportedForSubqueries(aggregationNode.getSource(), lookup)) {
                return SPLIT_TO_SUBQUERIES;
            }
            return SINGLE_STEP;
        }

        double numberOfDistinctValues = getMinDistinctValueCountEstimate(aggregationNode, statsProvider);
        int maxNumberOfConcurrentThreadsForAggregation = getMaxNumberOfConcurrentThreadsForAggregation(session);

        if (!aggregationNode.getGroupingKeys().isEmpty() &&
                !isNaN(numberOfDistinctValues) &&
                (numberOfDistinctValues > PRE_AGGREGATE_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation ||
                        (numberOfDistinctValues > MARK_DISTINCT_MAX_OUTPUT_ROW_COUNT_MULTIPLIER * maxNumberOfConcurrentThreadsForAggregation && aggregationNode.getGroupingKeys().size() > 2))) {
            return SINGLE_STEP;
        }

        if (isAggregationCandidateForSplittingToSubqueries(aggregationNode) && shouldSplitAggregationToSubqueries(aggregationNode, statsProvider, lookup)) {
            return SPLIT_TO_SUBQUERIES;
        }

        if (canUsePreAggregate(aggregationNode) && aggregationNode.getGroupingKeys().size() <= 2) {
            return PRE_AGGREGATE;
        }
        if (canUseMarkDistinct(aggregationNode)) {
            return MARK_DISTINCT;
        }

        return SINGLE_STEP;
    }

    private int getMaxNumberOfConcurrentThreadsForAggregation(Session session)
    {
        return taskCountEstimator.estimateHashedTaskCount(session) * getTaskConcurrency(session);
    }

    private double getMinDistinctValueCountEstimate(AggregationNode aggregationNode, StatsProvider statsProvider)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(aggregationNode.getSource());
        return aggregationNode.getGroupingKeys().stream()
                .filter(variable -> !isNaN(sourceStats.getVariableStatistics(variable).getDistinctValuesCount()))
                .map(variable -> sourceStats.getVariableStatistics(variable).getDistinctValuesCount())
                .max(Double::compareTo)
                .orElse(NaN);
    }

    private boolean shouldSplitAggregationToSubqueries(AggregationNode aggregationNode, StatsProvider statsProvider, Lookup lookup)
    {
        if (!isAggregationSourceSupportedForSubqueries(aggregationNode.getSource(), lookup)) {
            return false;
        }

        if (searchFrom(aggregationNode.getSource(), lookup).whereIsInstanceOfAny(ImmutableList.of(UnionNode.class)).findFirst().isPresent()) {
            return false;
        }

        if (searchFrom(aggregationNode.getSource(), lookup)
                .where(node -> node instanceof FilterNode && isSelective((FilterNode) node, statsProvider))
                .matches()) {
            return false;
        }

        if (isAdditionalReadOverheadTooExpensive(aggregationNode, statsProvider, lookup)) {
            return false;
        }

        if (aggregationNode.hasEmptyGroupingSet()) {
            return true;
        }

        PlanNodeStatsEstimate stats = statsProvider.getStats(aggregationNode);
        double groupingKeysSizeInBytes = stats.getOutputSizeForVariables(aggregationNode.getGroupingKeys());
        return !(isNaN(groupingKeysSizeInBytes) || groupingKeysSizeInBytes > MAX_JOIN_GROUPING_KEYS_SIZE);
    }

    private static boolean isAdditionalReadOverheadTooExpensive(AggregationNode aggregationNode, StatsProvider statsProvider, Lookup lookup)
    {
        Set<VariableReferenceExpression> distinctInputs = aggregationNode.getAggregations().values().stream()
                .filter(AggregationNode.Aggregation::isDistinct)
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .filter(VariableReferenceExpression.class::isInstance)
                .map(VariableReferenceExpression.class::cast)
                .collect(toImmutableSet());

        TableScanNode tableScanNode = (TableScanNode) searchFrom(aggregationNode.getSource(), lookup).whereIsInstanceOfAny(ImmutableList.of(TableScanNode.class)).findOnlyElement();
        Set<VariableReferenceExpression> additionalColumns = Sets.difference(ImmutableSet.copyOf(tableScanNode.getOutputVariables()), distinctInputs);

        double singleTableScanDataSize = statsProvider.getStats(tableScanNode).getOutputSizeForVariables(tableScanNode.getOutputVariables());
        double additionalColumnsDataSize = statsProvider.getStats(tableScanNode).getOutputSizeForVariables(additionalColumns);
        long subqueryCount = distinctAggregationsUniqueArgumentCount(aggregationNode);
        double distinctInputDataSize = singleTableScanDataSize - additionalColumnsDataSize;
        double subqueriesTotalDataSize = additionalColumnsDataSize * subqueryCount + distinctInputDataSize;

        return isNaN(subqueriesTotalDataSize) || isNaN(singleTableScanDataSize) || subqueriesTotalDataSize / singleTableScanDataSize > 1.5;
    }

    private static boolean isSelective(FilterNode filterNode, StatsProvider statsProvider)
    {
        double filterOutputRowCount = statsProvider.getStats(filterNode).getOutputRowCount();
        double filterSourceRowCount = statsProvider.getStats(filterNode.getSource()).getOutputRowCount();
        return filterOutputRowCount / filterSourceRowCount < 0.5;
    }

    private boolean isAggregationSourceSupportedForSubqueries(PlanNode source, Lookup lookup)
    {
        if (searchFrom(source, lookup)
                .where(node -> !(node instanceof TableScanNode || node instanceof FilterNode || node instanceof ProjectNode || node instanceof UnionNode))
                .findFirst()
                .isPresent()) {
            return false;
        }

        return !searchFrom(source, lookup).whereIsInstanceOfAny(ImmutableList.of(TableScanNode.class)).findAll().isEmpty();
    }
}
