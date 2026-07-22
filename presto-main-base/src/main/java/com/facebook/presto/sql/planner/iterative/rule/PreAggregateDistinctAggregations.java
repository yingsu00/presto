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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isOptimizeDistinctAggregationEnabled;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toCollection;

/**
 * Implements Trino's PRE_AGGREGATE distinct strategy for aggregations with
 * multiple single-argument distinct inputs, or mixed distinct and non-distinct
 * aggregations.
 */
public class PreAggregateDistinctAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PreAggregateDistinctAggregations::canUsePreAggregate);

    private final Metadata metadata;
    private final FunctionResolution functionResolution;

    public PreAggregateDistinctAggregations(Metadata metadata)
    {
        this.metadata = metadata;
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    public static boolean canUsePreAggregate(AggregationNode aggregation)
    {
        return (hasMultipleDistincts(aggregation) || hasMixedDistinctAndNonDistincts(aggregation)) &&
                allDistinctAggregationsHaveSingleArgument(aggregation) &&
                noFilters(aggregation) &&
                noMasks(aggregation) &&
                !aggregation.hasOrderings() &&
                aggregation.getStep().equals(SINGLE);
    }

    private static boolean hasMultipleDistincts(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .filter(Aggregation::isDistinct)
                .map(Aggregation::getArguments)
                .map(HashSet::new)
                .distinct()
                .count() > 1;
    }

    private static boolean hasMixedDistinctAndNonDistincts(AggregationNode aggregation)
    {
        long distincts = aggregation.getAggregations().values().stream()
                .filter(Aggregation::isDistinct)
                .count();
        return distincts > 0 && distincts < aggregation.getAggregations().size();
    }

    private static boolean allDistinctAggregationsHaveSingleArgument(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .filter(Aggregation::isDistinct)
                .allMatch(node -> node.getArguments().size() == 1);
    }

    private static boolean noFilters(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .noneMatch(node -> node.getFilter().isPresent());
    }

    private static boolean noMasks(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .noneMatch(node -> node.getMask().isPresent());
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        Session session = context.getSession();
        if (!isOptimizeDistinctAggregationEnabled(session)) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> distinctArguments = node.getAggregations().values().stream()
                .filter(Aggregation::isDistinct)
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .map(VariableReferenceExpression.class::cast)
                .collect(toCollection(LinkedHashSet::new));
        boolean hasNonDistinctAggregation = node.getAggregations().values().stream().anyMatch(aggregation -> !aggregation.isDistinct());

        Map<VariableReferenceExpression, Integer> distinctArgumentToGroupId = new LinkedHashMap<>();
        int nextGroupId = hasNonDistinctAggregation ? 1 : 0;
        for (VariableReferenceExpression distinctArgument : distinctArguments) {
            distinctArgumentToGroupId.put(distinctArgument, nextGroupId++);
        }

        Map<VariableReferenceExpression, VariableReferenceExpression> groupIdOutputToInput = new LinkedHashMap<>();
        distinctArguments.forEach(variable -> groupIdOutputToInput.put(variable, variable));
        node.getGroupingKeys().forEach(variable -> groupIdOutputToInput.put(variable, variable));

        VariableReferenceExpression groupVariable = context.getVariableAllocator().newVariable("group", BIGINT);
        Assignments.Builder filterAssignments = Assignments.builder();
        VariableReferenceExpression nonDistinctGroupFilter = context.getVariableAllocator().newVariable("non_distinct_gid_filter", BOOLEAN);
        if (hasNonDistinctAggregation) {
            filterAssignments.put(nonDistinctGroupFilter, groupEquals(groupVariable, 0));
        }

        Map<VariableReferenceExpression, Aggregation> outerAggregations = new LinkedHashMap<>();
        Map<Integer, VariableReferenceExpression> groupIdFilterByGroupId = new HashMap<>();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            if (originalAggregation.isDistinct()) {
                VariableReferenceExpression aggregationInput = (VariableReferenceExpression) originalAggregation.getArguments().get(0);
                int groupId = distinctArgumentToGroupId.get(aggregationInput);
                VariableReferenceExpression groupIdFilter = groupIdFilterByGroupId.computeIfAbsent(groupId, id -> {
                    VariableReferenceExpression filter = context.getVariableAllocator().newVariable("gid_filter_" + id, BOOLEAN);
                    filterAssignments.put(filter, groupEquals(groupVariable, id));
                    return filter;
                });
                outerAggregations.put(entry.getKey(), new Aggregation(
                        originalAggregation.getCall(),
                        Optional.of(groupIdFilter),
                        Optional.empty(),
                        false,
                        Optional.empty()));
            }
        }

        Map<VariableReferenceExpression, Aggregation> innerAggregations = new LinkedHashMap<>();
        Map<VariableReferenceExpression, VariableReferenceExpression> coalesceVariables = new LinkedHashMap<>();
        ImmutableSet.Builder<VariableReferenceExpression> nonDistinctAggregationArguments = ImmutableSet.builder();
        Map<VariableReferenceExpression, VariableReferenceExpression> duplicatedGroupIdInputToOutput = new HashMap<>();
        if (hasNonDistinctAggregation) {
            for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
                Aggregation originalAggregation = entry.getValue();
                if (!originalAggregation.isDistinct()) {
                    ImmutableList.Builder<RowExpression> mappedArguments = ImmutableList.builder();
                    for (RowExpression argument : originalAggregation.getArguments()) {
                        VariableReferenceExpression argumentVariable = (VariableReferenceExpression) argument;
                        VariableReferenceExpression finalArgument = argumentVariable;
                        if (distinctArguments.contains(argumentVariable)) {
                            finalArgument = duplicatedGroupIdInputToOutput.computeIfAbsent(
                                    argumentVariable,
                                    variable -> context.getVariableAllocator().newVariable("gid_non_distinct", variable.getType()));
                        }
                        groupIdOutputToInput.put(finalArgument, argumentVariable);
                        mappedArguments.add(finalArgument);
                        nonDistinctAggregationArguments.add(finalArgument);
                    }

                    VariableReferenceExpression innerAggregationOutput = context.getVariableAllocator().newVariable("inner", entry.getKey().getType());
                    innerAggregations.put(innerAggregationOutput, new Aggregation(
                            new CallExpression(
                                    originalAggregation.getCall().getSourceLocation(),
                                    originalAggregation.getCall().getDisplayName(),
                                    originalAggregation.getCall().getFunctionHandle(),
                                    originalAggregation.getCall().getType(),
                                    mappedArguments.build()),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));

                    Aggregation outerAggregation = new Aggregation(
                            new CallExpression(
                                    originalAggregation.getCall().getSourceLocation(),
                                    "arbitrary",
                                    metadata.getFunctionAndTypeManager().lookupFunction("arbitrary", fromTypes(ImmutableList.of(innerAggregationOutput.getType()))),
                                    entry.getKey().getType(),
                                    ImmutableList.of(innerAggregationOutput)),
                            Optional.of(nonDistinctGroupFilter),
                            Optional.empty(),
                            false,
                            Optional.empty());

                    VariableReferenceExpression outerAggregationOutput = entry.getKey();
                    FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
                    if (functionResolution.isCountFunction(functionHandle) ||
                            functionResolution.isCountIfFunction(functionHandle) ||
                            functionResolution.isApproximateCountDistinctFunction(functionHandle)) {
                        outerAggregationOutput = context.getVariableAllocator().newVariable("coalesce_expr", entry.getKey().getType());
                        coalesceVariables.put(outerAggregationOutput, entry.getKey());
                    }
                    outerAggregations.put(outerAggregationOutput, outerAggregation);
                }
            }
        }

        GroupIdNode groupIdNode = new GroupIdNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                node.getSource(),
                createGroups(node.getGroupingKeys(), nonDistinctAggregationArguments.build(), hasNonDistinctAggregation, distinctArgumentToGroupId),
                ImmutableMap.copyOf(groupIdOutputToInput),
                ImmutableList.of(),
                groupVariable);

        Set<VariableReferenceExpression> innerGroupingKeys = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(node.getGroupingKeys())
                .addAll(distinctArguments)
                .add(groupVariable)
                .build();
        AggregationNode innerAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                groupIdNode,
                ImmutableMap.copyOf(innerAggregations),
                singleGroupingSet(ImmutableList.copyOf(innerGroupingKeys)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        for (VariableReferenceExpression variable : innerAggregation.getOutputVariables()) {
            filterAssignments.put(variable, variable);
        }
        ProjectNode filtersProject = new ProjectNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                innerAggregation,
                filterAssignments.build(),
                LOCAL);

        AggregationNode outerAggregation = new AggregationNode(
                node.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filtersProject,
                ImmutableMap.copyOf(outerAggregations),
                node.getGroupingSets(),
                ImmutableList.of(),
                node.getStep(),
                Optional.empty(),
                node.getGroupIdVariable(),
                node.getAggregationId());

        if (coalesceVariables.isEmpty()) {
            return Result.ofPlanNode(outerAggregation);
        }

        Assignments.Builder outputVariables = Assignments.builder();
        for (VariableReferenceExpression variable : outerAggregation.getOutputVariables()) {
            if (coalesceVariables.containsKey(variable)) {
                outputVariables.put(coalesceVariables.get(variable), new SpecialFormExpression(variable.getSourceLocation(), COALESCE, BIGINT, variable, constant(0L, BIGINT)));
            }
            else {
                outputVariables.put(variable, variable);
            }
        }

        return Result.ofPlanNode(new ProjectNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), outerAggregation, outputVariables.build(), LOCAL));
    }

    private RowExpression groupEquals(VariableReferenceExpression groupVariable, int groupId)
    {
        return call(
                EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, BIGINT, BIGINT),
                BOOLEAN,
                ImmutableList.of(groupVariable, constant((long) groupId, BIGINT)));
    }

    private static List<List<VariableReferenceExpression>> createGroups(
            List<VariableReferenceExpression> groupingKeys,
            Set<VariableReferenceExpression> nonDistinctAggregationArguments,
            boolean hasNonDistinctAggregation,
            Map<VariableReferenceExpression, Integer> distinctArgumentToGroupId)
    {
        ImmutableList.Builder<List<VariableReferenceExpression>> groups = ImmutableList.builder();

        if (hasNonDistinctAggregation) {
            groups.add(ImmutableList.copyOf(ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(groupingKeys)
                    .addAll(nonDistinctAggregationArguments)
                    .build()));
        }

        distinctArgumentToGroupId.entrySet().stream()
                .sorted(comparing(Map.Entry::getValue))
                .forEach(entry -> groups.add(ImmutableList.copyOf(ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(groupingKeys)
                        .add(entry.getKey())
                        .build())));

        return groups.build();
    }
}
