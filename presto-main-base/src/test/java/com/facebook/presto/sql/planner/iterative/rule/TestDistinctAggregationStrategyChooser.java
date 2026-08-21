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
import com.facebook.presto.cost.CostProvider;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.LogicalPropertiesProvider;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDistinctAggregationStrategyChooser
        extends BaseRuleTest
{
    private static final int NODE_COUNT = 6;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODE_COUNT);

    @Test
    public void testSingleStepPreferredForHighCardinalitySingleGroupByKey()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression groupingKey = variableAllocator.newVariable("groupingKey", BIGINT);

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, variableAllocator);
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(ImmutableMap.of(groupingKey, 1_000_000.0))),
                variableAllocator);

        assertShouldUseSingleStep(aggregationNode, context);
    }

    @Test
    public void testSingleStepPreferredForHighCardinalityMultipleGroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression lowCardinalityGroupingKey = variableAllocator.newVariable("lowCardinalityGroupingKey", BIGINT);
        VariableReferenceExpression highCardinalityGroupingKey = variableAllocator.newVariable("highCardinalityGroupingKey", BIGINT);

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(
                ImmutableList.of(lowCardinalityGroupingKey, highCardinalityGroupingKey),
                source,
                variableAllocator);
        // the maximum NDV over all grouping keys is used, so a single high cardinality key is enough
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(ImmutableMap.of(
                        lowCardinalityGroupingKey, 10.0,
                        highCardinalityGroupingKey, 1_000_000.0))),
                variableAllocator);

        assertShouldUseSingleStep(aggregationNode, context);
    }

    @Test
    public void testPreAggregatePreferredForLowCardinality2GroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        List<VariableReferenceExpression> groupingKeys = ImmutableList.of(
                variableAllocator.newVariable("key1", BIGINT),
                variableAllocator.newVariable("key2", BIGINT));

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, variableAllocator);
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(groupingKeys.stream()
                        .collect(toImmutableMap(Function.identity(), key -> 10.0)))),
                variableAllocator);

        assertTrue(shouldUsePreAggregate(aggregationNode, context));
        assertFalse(shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testPreAggregatePreferredForUnknownStatisticsAnd2GroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        List<VariableReferenceExpression> groupingKeys = ImmutableList.of(
                variableAllocator.newVariable("key1", BIGINT),
                variableAllocator.newVariable("key2", BIGINT));

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, variableAllocator);
        Rule.Context context = context(ImmutableMap.of(), variableAllocator);

        assertTrue(shouldUsePreAggregate(aggregationNode, context));
        assertFalse(shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testPreAggregatePreferredForMediumCardinalitySingleGroupByKey()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression groupingKey = variableAllocator.newVariable("groupingKey", BIGINT);

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, variableAllocator);
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(ImmutableMap.of(groupingKey, 10.0 * clusterThreadCount()))),
                variableAllocator);

        assertTrue(shouldUsePreAggregate(aggregationNode, context));
    }

    @Test
    public void testSingleStepPreferredForMediumCardinality3GroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        List<VariableReferenceExpression> groupingKeys = ImmutableList.of(
                variableAllocator.newVariable("key1", BIGINT),
                variableAllocator.newVariable("key2", BIGINT),
                variableAllocator.newVariable("key3", BIGINT));

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, variableAllocator);
        // with more than 2 grouping keys, pre-aggregate adds the keys to every grouping set, so single-step wins earlier
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(groupingKeys.stream()
                        .collect(toImmutableMap(Function.identity(), key -> 10.0 * clusterThreadCount())))),
                variableAllocator);

        assertShouldUseSingleStep(aggregationNode, context);
    }

    @Test
    public void testMarkDistinctPreferredForLowCardinality3GroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        List<VariableReferenceExpression> groupingKeys = ImmutableList.of(
                variableAllocator.newVariable("key1", BIGINT),
                variableAllocator.newVariable("key2", BIGINT),
                variableAllocator.newVariable("key3", BIGINT));

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, variableAllocator);
        Rule.Context context = context(
                ImmutableMap.of(source, statsWithDistinctValueCounts(groupingKeys.stream()
                        .collect(toImmutableMap(Function.identity(), key -> 10.0)))),
                variableAllocator);

        assertTrue(shouldAddMarkDistinct(aggregationNode, context));
        assertFalse(shouldUsePreAggregate(aggregationNode, context));
    }

    @Test
    public void testMarkDistinctPreferredForUnknownStatisticsAnd3GroupByKeys()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        List<VariableReferenceExpression> groupingKeys = ImmutableList.of(
                variableAllocator.newVariable("key1", BIGINT),
                variableAllocator.newVariable("key2", BIGINT),
                variableAllocator.newVariable("key3", BIGINT));

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(groupingKeys, source, variableAllocator);
        Rule.Context context = context(ImmutableMap.of(), variableAllocator);

        assertTrue(shouldAddMarkDistinct(aggregationNode, context));
    }

    @Test
    public void testChoiceForcedByTheSessionProperty()
    {
        VariableAllocator variableAllocator = new VariableAllocator();
        VariableReferenceExpression groupingKey = variableAllocator.newVariable("groupingKey", BIGINT);

        PlanNode source = values(variableAllocator);
        AggregationNode aggregationNode = aggregationWithTwoDistinctAggregations(ImmutableList.of(groupingKey), source, variableAllocator);
        // an NDV high enough that AUTOMATIC would choose single-step
        Map<PlanNode, PlanNodeStatsEstimate> stats = ImmutableMap.of(
                source, statsWithDistinctValueCounts(ImmutableMap.of(groupingKey, 1000.0 * clusterThreadCount())));

        // big NDV, distinct_aggregations_strategy = MARK_DISTINCT
        Rule.Context markDistinctContext = context(stats, variableAllocator, session("MARK_DISTINCT"));
        assertTrue(shouldAddMarkDistinct(aggregationNode, markDistinctContext));

        // big NDV, distinct_aggregations_strategy = PRE_AGGREGATE
        Rule.Context preAggregateContext = context(stats, variableAllocator, session("PRE_AGGREGATE"));
        assertTrue(shouldUsePreAggregate(aggregationNode, preAggregateContext));

        // big NDV, distinct_aggregations_strategy = SINGLE_STEP
        assertShouldUseSingleStep(aggregationNode, context(stats, variableAllocator, session("SINGLE_STEP")));
    }

    private int clusterThreadCount()
    {
        return NODE_COUNT * getTaskConcurrency(tester().getSession());
    }

    private boolean shouldAddMarkDistinct(AggregationNode aggregationNode, Rule.Context context)
    {
        return createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR)
                .shouldAddMarkDistinct(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
    }

    private boolean shouldUsePreAggregate(AggregationNode aggregationNode, Rule.Context context)
    {
        return createDistinctAggregationStrategyChooser(TASK_COUNT_ESTIMATOR)
                .shouldUsePreAggregate(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup());
    }

    private void assertShouldUseSingleStep(AggregationNode aggregationNode, Rule.Context context)
    {
        assertFalse(shouldAddMarkDistinct(aggregationNode, context));
        assertFalse(shouldUsePreAggregate(aggregationNode, context));
    }

    private Session session(String distinctAggregationsStrategy)
    {
        return Session.builder(tester().getSession())
                .setSystemProperty(DISTINCT_AGGREGATIONS_STRATEGY, distinctAggregationsStrategy)
                .build();
    }

    private static PlanNodeStatsEstimate statsWithDistinctValueCounts(Map<VariableReferenceExpression, Double> distinctValueCounts)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder().setOutputRowCount(1_000_000);
        distinctValueCounts.forEach((variable, distinctValuesCount) ->
                builder.addVariableStatistics(variable, VariableStatsEstimate.builder().setDistinctValuesCount(distinctValuesCount).build()));
        return builder.build();
    }

    private static ValuesNode values(VariableAllocator variableAllocator)
    {
        return new ValuesNode(
                Optional.empty(),
                new PlanNodeId("source"),
                ImmutableList.of(variableAllocator.newVariable("unused", BIGINT)),
                ImmutableList.of(),
                Optional.empty());
    }

    private AggregationNode aggregationWithTwoDistinctAggregations(List<VariableReferenceExpression> groupingKeys, PlanNode source, VariableAllocator variableAllocator)
    {
        return aggregation(
                groupingKeys,
                source,
                variableAllocator.newVariable("input1", BIGINT),
                variableAllocator.newVariable("input2", BIGINT),
                variableAllocator);
    }

    private AggregationNode aggregation(
            List<VariableReferenceExpression> groupingKeys,
            PlanNode source,
            VariableReferenceExpression firstDistinctInput,
            VariableReferenceExpression secondDistinctInput,
            VariableAllocator variableAllocator)
    {
        return new AggregationNode(
                Optional.empty(),
                new PlanNodeId("aggregation"),
                source,
                ImmutableMap.of(
                        variableAllocator.newVariable("output1", BIGINT), distinctSum(firstDistinctInput),
                        variableAllocator.newVariable("output2", BIGINT), distinctSum(secondDistinctInput)),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private Aggregation distinctSum(VariableReferenceExpression input)
    {
        return new Aggregation(
                new CallExpression(
                        "sum",
                        getMetadata().getFunctionAndTypeManager().lookupFunction("sum", fromTypes(BIGINT)),
                        BIGINT,
                        ImmutableList.of(input)),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty());
    }

    private Rule.Context context(Map<PlanNode, PlanNodeStatsEstimate> stats, VariableAllocator variableAllocator)
    {
        // the strategy is set explicitly so that these tests do not depend on the configured default
        return context(stats, variableAllocator, session("AUTOMATIC"));
    }

    private Rule.Context context(Map<PlanNode, PlanNodeStatsEstimate> stats, VariableAllocator variableAllocator, Session session)
    {
        PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return Lookup.noLookup();
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return planNodeIdAllocator;
            }

            @Override
            public VariableAllocator getVariableAllocator()
            {
                return variableAllocator;
            }

            @Override
            public Session getSession()
            {
                return session;
            }

            @Override
            public StatsProvider getStatsProvider()
            {
                return node -> stats.getOrDefault(node, PlanNodeStatsEstimate.unknown());
            }

            @Override
            public CostProvider getCostProvider()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void checkTimeoutNotExhausted()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public WarningCollector getWarningCollector()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Optional<LogicalPropertiesProvider> getLogicalPropertiesProvider()
            {
                return Optional.empty();
            }
        };
    }
}
