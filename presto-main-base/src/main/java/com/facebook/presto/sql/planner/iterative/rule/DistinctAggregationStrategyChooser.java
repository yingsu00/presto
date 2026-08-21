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
import com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy;
import com.facebook.presto.sql.planner.iterative.Lookup;

import static com.facebook.presto.SystemSessionProperties.distinctAggregationsStrategy;
import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.MARK_DISTINCT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.PRE_AGGREGATE;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.DistinctAggregationsStrategy.SINGLE_STEP;
import static com.facebook.presto.sql.planner.iterative.rule.MultipleDistinctAggregationToMarkDistinct.canUseMarkDistinct;
import static com.facebook.presto.sql.planner.iterative.rule.PreAggregateDistinctAggregations.canUsePreAggregate;
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
        return chooseDistinctAggregationStrategy(aggregationNode, session, statsProvider, lookup) == MARK_DISTINCT;
    }

    public boolean shouldUsePreAggregate(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        return chooseDistinctAggregationStrategy(aggregationNode, session, statsProvider, lookup) == PRE_AGGREGATE;
    }

    private DistinctAggregationsStrategy chooseDistinctAggregationStrategy(AggregationNode aggregationNode, Session session, StatsProvider statsProvider, Lookup lookup)
    {
        DistinctAggregationsStrategy distinctAggregationsStrategy = distinctAggregationsStrategy(session);
        if (distinctAggregationsStrategy != AUTOMATIC) {
            if (distinctAggregationsStrategy == MARK_DISTINCT && canUseMarkDistinct(aggregationNode)) {
                return MARK_DISTINCT;
            }
            if (distinctAggregationsStrategy == PRE_AGGREGATE && canUsePreAggregate(aggregationNode)) {
                return PRE_AGGREGATE;
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
        // NDV stats for multiple grouping keys are unreliable, so pick a conservative lower bound by taking the maximum NDV over all grouping keys.
        // This assumes the grouping keys are 100% correlated; with a lower correlation the real NDV can only be higher.
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(aggregationNode.getSource());
        return aggregationNode.getGroupingKeys().stream()
                .filter(variable -> !isNaN(sourceStats.getVariableStatistics(variable).getDistinctValuesCount()))
                .map(variable -> sourceStats.getVariableStatistics(variable).getDistinctValuesCount())
                .max(Double::compareTo)
                .orElse(NaN);
    }
}
