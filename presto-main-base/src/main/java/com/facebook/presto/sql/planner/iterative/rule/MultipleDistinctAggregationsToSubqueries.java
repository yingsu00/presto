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

import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.sql.planner.iterative.rule.DistinctAggregationStrategyChooser.createDistinctAggregationStrategyChooser;
import static com.facebook.presto.sql.planner.iterative.rule.PreAggregateDistinctAggregations.allDistinctAggregates;
import static com.facebook.presto.sql.planner.iterative.rule.PreAggregateDistinctAggregations.hasMultipleDistincts;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Splits multiple distinct aggregations into subqueries joined on grouping keys.
 */
public class MultipleDistinctAggregationsToSubqueries
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MultipleDistinctAggregationsToSubqueries::isAggregationCandidateForSplittingToSubqueries);

    private final DistinctAggregationStrategyChooser distinctAggregationStrategyChooser;

    public MultipleDistinctAggregationsToSubqueries()
    {
        this(new TaskCountEstimator(() -> 1));
    }

    public MultipleDistinctAggregationsToSubqueries(TaskCountEstimator taskCountEstimator)
    {
        this.distinctAggregationStrategyChooser = createDistinctAggregationStrategyChooser(taskCountEstimator);
    }

    public static boolean isAggregationCandidateForSplittingToSubqueries(AggregationNode aggregationNode)
    {
        return allDistinctAggregates(aggregationNode) &&
                hasMultipleDistincts(aggregationNode) &&
                aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        if (!distinctAggregationStrategyChooser.shouldSplitToSubqueries(aggregationNode, context.getSession(), context.getStatsProvider(), context.getLookup())) {
            return Result.empty();
        }

        Map<Set<RowExpression>, Map<VariableReferenceExpression, Aggregation>> aggregationsByArguments = new LinkedHashMap<>(aggregationNode.getAggregations().size());
        List<Entry<VariableReferenceExpression, Aggregation>> sortedAggregations = aggregationNode.getAggregations().entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().getName()))
                .collect(toImmutableList());
        for (Entry<VariableReferenceExpression, Aggregation> entry : sortedAggregations) {
            Set<RowExpression> arguments = ImmutableSet.copyOf(entry.getValue().getArguments());
            aggregationsByArguments.computeIfAbsent(arguments, ignored -> new LinkedHashMap<>()).put(entry.getKey(), entry.getValue());
        }

        PlanNode right = null;
        List<VariableReferenceExpression> rightJoinVariables = null;
        Assignments.Builder assignments = Assignments.builder();
        List<Map<VariableReferenceExpression, Aggregation>> aggregationsByArgumentsList = ImmutableList.copyOf(aggregationsByArguments.values());
        for (int i = aggregationsByArgumentsList.size() - 1; i > 0; i--) {
            AggregationNode subAggregationNode = buildSubAggregation(aggregationNode, aggregationsByArgumentsList.get(i), assignments, context);
            if (right == null) {
                right = subAggregationNode;
                rightJoinVariables = subAggregationNode.getGroupingKeys();
            }
            else {
                right = buildJoin(subAggregationNode, subAggregationNode.getGroupingKeys(), right, rightJoinVariables, context);
            }
        }

        AggregationNode left = buildSubAggregation(aggregationNode, aggregationsByArgumentsList.get(0), assignments, context);
        for (int i = 0; i < left.getGroupingKeys().size(); i++) {
            assignments.put(aggregationNode.getGroupingKeys().get(i), left.getGroupingKeys().get(i));
        }

        JoinNode topJoin = buildJoin(left, left.getGroupingKeys(), right, rightJoinVariables, context);
        return Result.ofPlanNode(new ProjectNode(aggregationNode.getSourceLocation(), aggregationNode.getId(), topJoin, assignments.build(), ProjectNode.Locality.LOCAL));
    }

    private AggregationNode buildSubAggregation(AggregationNode aggregationNode, Map<VariableReferenceExpression, Aggregation> aggregations, Assignments.Builder assignments, Context context)
    {
        List<VariableReferenceExpression> originalAggregationOutputVariables = ImmutableList.copyOf(aggregations.keySet());
        NodeAndMappings copied = copyPlan(
                new AggregationNode(
                        aggregationNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        aggregationNode.getSource(),
                        aggregations,
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedVariables(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashVariable(),
                        aggregationNode.getGroupIdVariable(),
                        aggregationNode.getAggregationId()),
                originalAggregationOutputVariables,
                context.getVariableAllocator(),
                context.getIdAllocator(),
                context.getLookup());

        for (int i = 0; i < originalAggregationOutputVariables.size(); i++) {
            assignments.put(originalAggregationOutputVariables.get(i), copied.getFields().get(i));
        }
        return (AggregationNode) copied.getNode();
    }

    private JoinNode buildJoin(PlanNode left, List<VariableReferenceExpression> leftJoinVariables, PlanNode right, List<VariableReferenceExpression> rightJoinVariables, Context context)
    {
        checkArgument(leftJoinVariables.size() == rightJoinVariables.size());
        List<EquiJoinClause> criteria = IntStream.range(0, leftJoinVariables.size())
                .mapToObj(i -> new EquiJoinClause(leftJoinVariables.get(i), rightJoinVariables.get(i)))
                .collect(toImmutableList());
        List<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(left.getOutputVariables())
                .addAll(right.getOutputVariables())
                .build();

        return new JoinNode(
                left.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                Optional.empty(),
                JoinType.INNER,
                left,
                right,
                criteria,
                outputs,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    private static NodeAndMappings copyPlan(PlanNode plan, List<VariableReferenceExpression> fields, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        Copier copier = new Copier(variableAllocator, idAllocator, lookup);
        PlanNode copy = SimplePlanRewriter.rewriteWith(copier, plan, null);
        List<VariableReferenceExpression> copiedFields = fields.stream()
                .map(copier::variableFor)
                .collect(toImmutableList());
        return new NodeAndMappings(copy, copiedFields);
    }

    private static class NodeAndMappings
    {
        private final PlanNode node;
        private final List<VariableReferenceExpression> fields;

        private NodeAndMappings(PlanNode node, List<VariableReferenceExpression> fields)
        {
            this.node = requireNonNull(node, "node is null");
            this.fields = requireNonNull(fields, "fields is null");
        }

        private PlanNode getNode()
        {
            return node;
        }

        private List<VariableReferenceExpression> getFields()
        {
            return fields;
        }
    }

    private static class Copier
            extends SimplePlanRewriter<Void>
    {
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Lookup lookup;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping = new HashMap<>();

        private Copier(VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
        {
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        private VariableReferenceExpression variableFor(VariableReferenceExpression variable)
        {
            return variableMapping.computeIfAbsent(variable, variableAllocator::newVariable);
        }

        private RowExpression rewrite(RowExpression expression)
        {
            return RowExpressionVariableInliner.inlineVariables(variable -> variableMapping.getOrDefault(variable, variable), expression);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            throw new UnsupportedOperationException("plan copying not implemented for " + node.getClass().getSimpleName());
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, RewriteContext<Void> context)
        {
            return context.rewrite(lookup.resolve(node));
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Map<VariableReferenceExpression, Aggregation> aggregations = new LinkedHashMap<>();
            node.getAggregations().forEach((variable, aggregation) -> aggregations.put(variableFor(variable), rewriteAggregation(aggregation)));
            return new AggregationNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    source,
                    aggregations,
                    AggregationNode.groupingSets(rewriteVariables(node.getGroupingKeys()), node.getGroupingSetCount(), node.getGlobalGroupingSets()),
                    rewriteVariables(node.getPreGroupedVariables()),
                    node.getStep(),
                    node.getHashVariable().map(this::variableFor),
                    node.getGroupIdVariable().map(this::variableFor),
                    node.getAggregationId());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            return new FilterNode(node.getSourceLocation(), idAllocator.getNextId(), context.rewrite(node.getSource()), rewrite(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            Assignments.Builder assignments = Assignments.builder();
            node.getAssignments().forEach((variable, expression) -> assignments.put(variableFor(variable), rewrite(expression)));
            return new ProjectNode(node.getSourceLocation(), idAllocator.getNextId(), source, assignments.build(), node.getLocality());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Map<VariableReferenceExpression, ColumnHandle> assignments = new LinkedHashMap<>();
            node.getAssignments().forEach((variable, column) -> assignments.put(variableFor(variable), column));
            return new TableScanNode(
                    node.getSourceLocation(),
                    idAllocator.getNextId(),
                    node.getTable(),
                    rewriteVariables(node.getOutputVariables()),
                    assignments,
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = node.getSources().stream()
                    .map(context::rewrite)
                    .collect(toImmutableList());
            Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping = new LinkedHashMap<>();
            node.getVariableMapping().forEach((variable, inputs) -> variableMapping.put(variableFor(variable), rewriteVariables(inputs)));
            return new UnionNode(node.getSourceLocation(), idAllocator.getNextId(), sources, rewriteVariables(node.getOutputVariables()), variableMapping);
        }

        private Aggregation rewriteAggregation(Aggregation aggregation)
        {
            CallExpression call = aggregation.getCall();
            CallExpression rewrittenCall = new CallExpression(
                    call.getSourceLocation(),
                    call.getDisplayName(),
                    call.getFunctionHandle(),
                    call.getType(),
                    call.getArguments().stream().map(this::rewrite).collect(toImmutableList()));
            return new Aggregation(
                    rewrittenCall,
                    aggregation.getFilter().map(this::rewrite),
                    aggregation.getOrderBy().map(this::rewriteOrderingScheme),
                    aggregation.isDistinct(),
                    aggregation.getMask().map(this::variableFor));
        }

        private OrderingScheme rewriteOrderingScheme(OrderingScheme orderingScheme)
        {
            return new OrderingScheme(orderingScheme.getOrderBy().stream()
                    .map(ordering -> new Ordering(variableFor(ordering.getVariable()), ordering.getSortOrder()))
                    .collect(toImmutableList()));
        }

        private List<VariableReferenceExpression> rewriteVariables(List<VariableReferenceExpression> variables)
        {
            return variables.stream().map(this::variableFor).collect(toImmutableList());
        }
    }
}
