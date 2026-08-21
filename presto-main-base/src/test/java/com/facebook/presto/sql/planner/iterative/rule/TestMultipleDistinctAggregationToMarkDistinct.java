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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestMultipleDistinctAggregationToMarkDistinct
        extends BaseRuleTest
{
    private static final int NODE_COUNT = 6;
    private static final TaskCountEstimator TASK_COUNT_ESTIMATOR = new TaskCountEstimator(() -> NODE_COUNT);

    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(input1)"))
                        .addAggregation(p.variable("output2"), p.rowExpression("count(input2)"))))
                .doesNotFire();
    }

    @Test
    public void testSingleDistinct()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input1)"), true)))
                .doesNotFire();
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("sum(DISTINCT input)"), true)))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(
                                p.variable("output1"),
                                p.rowExpression("count(DISTINCT input1)"),
                                Optional.of(p.rowExpression("input2 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())
                        .addAggregation(
                                p.variable("output2"),
                                p.rowExpression("count(DISTINCT input2)"),
                                Optional.of(p.rowExpression("input1 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())))
                .doesNotFire();

        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(
                                p.variable("output1"),
                                p.rowExpression("count(DISTINCT input1)"),
                                Optional.of(p.rowExpression("input2 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())
                        .addAggregation(p.variable("output2"), p.rowExpression("count(DISTINCT input2)"), true)))
                .doesNotFire();
    }

    @Test
    public void testAggregationNDV()
    {
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        VariableReferenceExpression key1 = new VariableReferenceExpression(Optional.empty(), "key1", BIGINT);
        // three grouping keys, so that pre-aggregate is never preferred over mark-distinct
        Function<PlanBuilder, PlanNode> plan = p -> p.aggregation(builder -> builder
                .source(p.values(aggregationSourceId, p.variable("input"), p.variable("key1"), p.variable("key2"), p.variable("key3")))
                .singleGroupingSet(p.variable("key1"), p.variable("key2"), p.variable("key3"))
                .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input)"), true)
                .addAggregation(p.variable("output2"), p.rowExpression("sum(input)")));
        PlanMatchPattern expectedMarkDistinct = node(
                AggregationNode.class,
                node(
                        MarkDistinctNode.class,
                        values(ImmutableMap.of("input", 0, "key1", 1, "key2", 2, "key3", 3))));

        int clusterThreadCount = NODE_COUNT * getTaskConcurrency(tester().getSession());

        // small NDV
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 2 * clusterThreadCount))
                .on(plan)
                .matches(expectedMarkDistinct);

        // unknown estimate
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, Double.NaN))
                .on(plan)
                .matches(expectedMarkDistinct);

        // medium NDV, single-step is preferred because there are more than 2 grouping keys
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 50 * clusterThreadCount))
                .on(plan)
                .doesNotFire();

        // big NDV
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 1000 * clusterThreadCount))
                .on(plan)
                .doesNotFire();

        // big NDV, distinct_aggregations_strategy = MARK_DISTINCT
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "MARK_DISTINCT")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 1000 * clusterThreadCount))
                .on(plan)
                .matches(expectedMarkDistinct);

        // small NDV, distinct_aggregations_strategy = SINGLE_STEP
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "SINGLE_STEP")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 2 * clusterThreadCount))
                .on(plan)
                .doesNotFire();

        // small NDV, but the legacy use_mark_distinct property vetoes the rewrite
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.USE_MARK_DISTINCT, "false")
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key1, 2 * clusterThreadCount))
                .on(plan)
                .doesNotFire();
    }

    @Test
    public void testPreAggregatePreferredForSingleGroupingKey()
    {
        PlanNodeId aggregationSourceId = new PlanNodeId("aggregationSourceId");
        VariableReferenceExpression key = new VariableReferenceExpression(Optional.empty(), "key", BIGINT);
        int clusterThreadCount = NODE_COUNT * getTaskConcurrency(tester().getSession());

        // with a small NDV and a single grouping key the chooser prefers pre-aggregate, so this rule must step aside
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct(TASK_COUNT_ESTIMATOR))
                .setSystemProperty(SystemSessionProperties.DISTINCT_AGGREGATIONS_STRATEGY, "AUTOMATIC")
                .overrideStats(aggregationSourceId.toString(), statsWithDistinctValueCount(key, 2 * clusterThreadCount))
                .on(p -> p.aggregation(builder -> builder
                        .source(p.values(aggregationSourceId, p.variable("input"), p.variable("key")))
                        .singleGroupingSet(p.variable("key"))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("sum(input)"))))
                .doesNotFire();
    }

    private static PlanNodeStatsEstimate statsWithDistinctValueCount(VariableReferenceExpression variable, double distinctValuesCount)
    {
        return PlanNodeStatsEstimate.builder()
                .addVariableStatistics(variable, VariableStatsEstimate.builder().setDistinctValuesCount(distinctValuesCount).build())
                .build();
    }
}
