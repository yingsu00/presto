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
package com.facebook.presto.sql.gen;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.FieldDefinition;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.Parameter;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Scope;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.ForLoop;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.Work;
import com.facebook.presto.operator.project.ConstantPageProjection;
import com.facebook.presto.operator.project.GeneratedPageProjection;
import com.facebook.presto.operator.project.InputChannels;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter;
import com.facebook.presto.operator.project.PageFilter;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.operator.project.PageProjectionWithOutputs;
import com.facebook.presto.operator.project.SelectedPositions;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.SqlFunctionProperties;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.sql.gen.LambdaBytecodeGenerator.CompiledLambda;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.relational.Expressions;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.presto.bytecode.Access.FINAL;
import static com.facebook.presto.bytecode.Access.PRIVATE;
import static com.facebook.presto.bytecode.Access.PUBLIC;
import static com.facebook.presto.bytecode.Access.a;
import static com.facebook.presto.bytecode.Parameter.arg;
import static com.facebook.presto.bytecode.ParameterizedType.type;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.add;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.and;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantBoolean;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantInt;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantNull;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.invokeStatic;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.lessThan;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.newArray;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.not;
import static com.facebook.presto.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.gen.BytecodeUtils.invoke;
import static com.facebook.presto.sql.gen.LambdaBytecodeGenerator.generateMethodsForLambda;
import static com.facebook.presto.util.CompilerUtils.defineClass;
import static com.facebook.presto.util.CompilerUtils.makeClassName;
import static com.facebook.presto.util.Reflection.constructorMethodHandle;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PageFunctionCompiler
{
    private final Metadata metadata;
    private final DeterminismEvaluator determinismEvaluator;

    private final LoadingCache<CacheKey, Supplier<PageProjection>> projectionCache;
    private final LoadingCache<CacheKey, Supplier<PageFilter>> filterCache;

    private final CacheStatsMBean projectionCacheStats;
    private final CacheStatsMBean filterCacheStats;

    @Inject
    public PageFunctionCompiler(Metadata metadata, CompilerConfig config)
    {
        this(metadata, requireNonNull(config, "config is null").getExpressionCacheSize());
    }

    public PageFunctionCompiler(Metadata metadata, int expressionCacheSize)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata.getFunctionManager());

        if (expressionCacheSize > 0) {
            projectionCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(cacheKey -> compileProjectionInternal(cacheKey.sqlFunctionProperties, cacheKey.rowExpressions, Optional.empty())));
            projectionCacheStats = new CacheStatsMBean(projectionCache);
        }
        else {
            projectionCache = null;
            projectionCacheStats = null;
        }

        if (expressionCacheSize > 0) {
            filterCache = CacheBuilder.newBuilder()
                    .recordStats()
                    .maximumSize(expressionCacheSize)
                    .build(CacheLoader.from(cacheKey -> compileFilterInternal(cacheKey.sqlFunctionProperties, cacheKey.rowExpressions.get(0), Optional.empty())));
            filterCacheStats = new CacheStatsMBean(filterCache);
        }
        else {
            filterCache = null;
            filterCacheStats = null;
        }
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getProjectionCache()
    {
        return projectionCacheStats;
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getFilterCache()
    {
        return filterCacheStats;
    }

    public List<Supplier<PageProjectionWithOutputs>> compileProjections(SqlFunctionProperties sqlFunctionProperties, List<? extends RowExpression> projections, Optional<String> classNameSuffix)
    {
        return IntStream.range(0, projections.size())
                .mapToObj(outputChannel -> toPageProjectionWithOutputs(compileProjection(sqlFunctionProperties, projections.get(outputChannel), classNameSuffix), new int[] {outputChannel}))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    public Supplier<PageProjection> compileProjection(SqlFunctionProperties sqlFunctionProperties, RowExpression projection, Optional<String> classNameSuffix)
    {
        if (projection instanceof InputReferenceExpression) {
            InputReferenceExpression input = (InputReferenceExpression) projection;
            InputPageProjection projectionFunction = new InputPageProjection(input.getField());
            return () -> projectionFunction;
        }

        if (projection instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) projection;
            ConstantPageProjection projectionFunction = new ConstantPageProjection(constant.getValue(), constant.getType());
            return () -> projectionFunction;
        }

        if (projectionCache == null) {
            return compileProjectionInternal(sqlFunctionProperties, ImmutableList.of(projection), classNameSuffix);
        }
        return projectionCache.getUnchecked(new CacheKey(sqlFunctionProperties, ImmutableList.of(projection)));
    }

    private Supplier<PageProjectionWithOutputs> toPageProjectionWithOutputs(Supplier<PageProjection> pageProjection, int[] outputChannels)
    {
        return () -> new PageProjectionWithOutputs(pageProjection.get(), outputChannels);
    }

    private Supplier<PageProjection> compileProjectionInternal(SqlFunctionProperties sqlFunctionProperties, List<RowExpression> projections, Optional<String> classNameSuffix)
    {
        requireNonNull(projections, "projections is null");
        checkArgument(projections.stream().allMatch(projection -> projection instanceof CallExpression || projection instanceof SpecialFormExpression));

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(projections);
        List<RowExpression> rewrittenExpression = result.getRewrittenExpressions();

        CallSiteBinder callSiteBinder = new CallSiteBinder();

        // generate Work
        ClassDefinition pageProjectionWorkDefinition = definePageProjectWorkClass(sqlFunctionProperties, rewrittenExpression, callSiteBinder, classNameSuffix);

        Class<? extends Work> pageProjectionWorkClass;
        try {
            pageProjectionWorkClass = defineClass(pageProjectionWorkDefinition, Work.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, e);
        }

        return () -> new GeneratedPageProjection(
                rewrittenExpression,
                rewrittenExpression.stream().allMatch(determinismEvaluator::isDeterministic),
                result.getInputChannels(),
                constructorMethodHandle(pageProjectionWorkClass, List.class, SqlFunctionProperties.class, Page.class, SelectedPositions.class));
    }

    private static ParameterizedType generateProjectionWorkClassName(Optional<String> classNameSuffix)
    {
        return makeClassName("PageProjectionWork", classNameSuffix);
    }

    private ClassDefinition definePageProjectWorkClass(SqlFunctionProperties sqlFunctionProperties, List<RowExpression> projections, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateProjectionWorkClassName(classNameSuffix),
                type(Object.class),
                type(Work.class));

        FieldDefinition blockBuilderFields = classDefinition.declareField(a(PRIVATE), "blockBuilders", List.class);
        FieldDefinition propertiesField = classDefinition.declareField(a(PRIVATE), "properties", SqlFunctionProperties.class);
        FieldDefinition pageField = classDefinition.declareField(a(PRIVATE), "page", Page.class);
        FieldDefinition selectedPositionsField = classDefinition.declareField(a(PRIVATE), "selectedPositions", SelectedPositions.class);
        FieldDefinition nextIndexOrPositionField = classDefinition.declareField(a(PRIVATE), "nextIndexOrPosition", int.class);
        FieldDefinition resultField = classDefinition.declareField(a(PRIVATE), "result", List.class);

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        // process
        generateProcessMethod(classDefinition, blockBuilderFields, projections.size(), propertiesField, pageField, selectedPositionsField, nextIndexOrPositionField, resultField);

        // getResult
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getResult", type(Object.class), ImmutableList.of());
        method.getBody().append(method.getThis().getField(resultField)).ret(Object.class);

        // evaluate
        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, projections, metadata, sqlFunctionProperties, "");
        generateEvaluateMethod(sqlFunctionProperties, classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, projections, blockBuilderFields);

        // constructor
        Parameter blockBuilders = arg("blockBuilders", List.class);
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter selectedPositions = arg("selectedPositions", SelectedPositions.class);

        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC), blockBuilders, properties, page, selectedPositions);

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class)
                .append(thisVariable)
                .append(invokeStatic(ImmutableList.class, "copyOf", ImmutableList.class, blockBuilders.cast(Collection.class)))
                .putField(blockBuilderFields)
                .append(thisVariable.setField(propertiesField, properties))
                .append(thisVariable.setField(pageField, page))
                .append(thisVariable.setField(selectedPositionsField, selectedPositions))
                .append(thisVariable.setField(nextIndexOrPositionField, selectedPositions.invoke("getOffset", int.class)))
                .append(thisVariable.setField(resultField, constantNull(Block.class)));

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();

        return classDefinition;
    }

    private static MethodDefinition generateProcessMethod(
            ClassDefinition classDefinition,
            FieldDefinition blockBuilders,
            int blockBuilderSize,
            FieldDefinition properties,
            FieldDefinition page,
            FieldDefinition selectedPositions,
            FieldDefinition nextIndexOrPosition,
            FieldDefinition result)
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "process", type(boolean.class), ImmutableList.of());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable from = scope.declareVariable("from", body, thisVariable.getField(nextIndexOrPosition));
        Variable to = scope.declareVariable("to", body, add(thisVariable.getField(selectedPositions).invoke("getOffset", int.class), thisVariable.getField(selectedPositions).invoke("size", int.class)));
        Variable positions = scope.declareVariable(int[].class, "positions");
        Variable index = scope.declareVariable(int.class, "index");

        IfStatement ifStatement = new IfStatement()
                .condition(thisVariable.getField(selectedPositions).invoke("isList", boolean.class));
        body.append(ifStatement);

        ifStatement.ifTrue(new BytecodeBlock()
                .append(positions.set(thisVariable.getField(selectedPositions).invoke("getPositions", int[].class)))
                .append(new ForLoop("positions loop")
                        .initialize(index.set(from))
                        .condition(lessThan(index, to))
                        .update(index.increment())
                        .body(new BytecodeBlock()
                                .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(properties), thisVariable.getField(page), positions.getElement(index))))));

        ifStatement.ifFalse(new ForLoop("range based loop")
                .initialize(index.set(from))
                .condition(lessThan(index, to))
                .update(index.increment())
                .body(new BytecodeBlock()
                        .append(thisVariable.invoke("evaluate", void.class, thisVariable.getField(properties), thisVariable.getField(page), index))));

        Variable blocksBuilder = scope.declareVariable("blocksBuilder", body, invokeStatic(ImmutableList.class, "builder", ImmutableList.Builder.class));
        Variable iterator = scope.createTempVariable(int.class);
        ForLoop forLoop = new ForLoop("for (iterator = 0; iterator < this.blockBuilders.size(); iterator ++) blockBuildersBuilder.add(this.blockBuilders.get(iterator).builder();")
                .initialize(iterator.set(constantInt(0)))
                .condition(lessThan(iterator, constantInt(blockBuilderSize)))
                .update(iterator.increment())
                .body(new BytecodeBlock()
                        .append(blocksBuilder.invoke(
                                "add",
                                ImmutableList.Builder.class,
                                thisVariable.getField(blockBuilders).invoke("get", Object.class, iterator).cast(BlockBuilder.class).invoke("build", Block.class).cast(Object.class)).pop()));

        body.append(forLoop)
                .comment("result = blockBuildersBuilder.build(); return true")
                .append(thisVariable.setField(result, blocksBuilder.invoke("build", ImmutableList.class)))
                .push(true)
                .retBoolean();

        return method;
    }

    private MethodDefinition generateEvaluateMethod(
            SqlFunctionProperties sqlFunctionProperties,
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            List<RowExpression> projections,
            FieldDefinition blockBuilders)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "evaluate",
                type(void.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Projections: %s", Joiner.on(", ").join(projections));

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();
        Variable thisVariable = method.getThis();

        declareBlockVariables(projections, page, scope, body);

        scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata,
                sqlFunctionProperties,
                compiledLambdaMap);

        Variable outputBlockVariable = scope.createTempVariable(BlockBuilder.class);
        for (int i = 0; i < projections.size(); i++) {
            body.append(outputBlockVariable.set(thisVariable.getField(blockBuilders).invoke("get", Object.class, constantInt(i))))
                    .append(compiler.compile(projections.get(i), scope, Optional.of(outputBlockVariable)));
        }
        body.ret();
        return method;
    }

    public Supplier<PageFilter> compileFilter(SqlFunctionProperties sqlFunctionProperties, RowExpression filter, Optional<String> classNameSuffix)
    {
        if (filterCache == null) {
            return compileFilterInternal(sqlFunctionProperties, filter, classNameSuffix);
        }
        return filterCache.getUnchecked(new CacheKey(sqlFunctionProperties, ImmutableList.of(filter)));
    }

    private Supplier<PageFilter> compileFilterInternal(SqlFunctionProperties sqlFunctionProperties, RowExpression filter, Optional<String> classNameSuffix)
    {
        requireNonNull(filter, "filter is null");

        PageFieldsToInputParametersRewriter.Result result = rewritePageFieldsToInputParameters(filter);

        CallSiteBinder callSiteBinder = new CallSiteBinder();
        ClassDefinition classDefinition = defineFilterClass(sqlFunctionProperties, result.getRewrittenExpression(), result.getInputChannels(), callSiteBinder, classNameSuffix);

        Class<? extends PageFilter> functionClass;
        try {
            functionClass = defineClass(classDefinition, PageFilter.class, callSiteBinder.getBindings(), getClass().getClassLoader());
        }
        catch (Exception e) {
            throw new PrestoException(COMPILER_ERROR, filter.toString(), e.getCause());
        }

        return () -> {
            try {
                return functionClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException e) {
                throw new PrestoException(COMPILER_ERROR, e);
            }
        };
    }

    private static ParameterizedType generateFilterClassName(Optional<String> classNameSuffix)
    {
        return makeClassName(PageFilter.class.getSimpleName(), classNameSuffix);
    }

    private ClassDefinition defineFilterClass(SqlFunctionProperties sqlFunctionProperties, RowExpression filter, InputChannels inputChannels, CallSiteBinder callSiteBinder, Optional<String> classNameSuffix)
    {
        ClassDefinition classDefinition = new ClassDefinition(
                a(PUBLIC, FINAL),
                generateFilterClassName(classNameSuffix),
                type(Object.class),
                type(PageFilter.class));

        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(classDefinition, callSiteBinder);

        Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap = generateMethodsForLambda(classDefinition, callSiteBinder, cachedInstanceBinder, filter, metadata, sqlFunctionProperties);
        generateFilterMethod(sqlFunctionProperties, classDefinition, callSiteBinder, cachedInstanceBinder, compiledLambdaMap, filter);

        FieldDefinition selectedPositions = classDefinition.declareField(a(PRIVATE), "selectedPositions", boolean[].class);
        generatePageFilterMethod(classDefinition, selectedPositions);

        // isDeterministic
        classDefinition.declareMethod(a(PUBLIC), "isDeterministic", type(boolean.class))
                .getBody()
                .append(constantBoolean(determinismEvaluator.isDeterministic(filter)))
                .retBoolean();

        // getInputChannels
        classDefinition.declareMethod(a(PUBLIC), "getInputChannels", type(InputChannels.class))
                .getBody()
                .append(invoke(callSiteBinder.bind(inputChannels, InputChannels.class), "getInputChannels"))
                .retObject();

        // toString
        String toStringResult = toStringHelper(classDefinition.getType()
                .getJavaClassName())
                .add("filter", filter)
                .toString();
        classDefinition.declareMethod(a(PUBLIC), "toString", type(String.class))
                .getBody()
                // bind constant via invokedynamic to avoid constant pool issues due to large strings
                .append(invoke(callSiteBinder.bind(toStringResult, String.class), "toString"))
                .retObject();

        // constructor
        generateConstructor(classDefinition, cachedInstanceBinder, compiledLambdaMap, method -> {
            Variable thisVariable = method.getScope().getThis();
            method.getBody().append(thisVariable.setField(selectedPositions, newArray(type(boolean[].class), 0)));
        });

        return classDefinition;
    }

    private static MethodDefinition generatePageFilterMethod(ClassDefinition classDefinition, FieldDefinition selectedPositionsField)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(SelectedPositions.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .build());

        Scope scope = method.getScope();
        Variable thisVariable = method.getThis();
        BytecodeBlock body = method.getBody();

        Variable positionCount = scope.declareVariable("positionCount", body, page.invoke("getPositionCount", int.class));

        body.append(new IfStatement("grow selectedPositions if necessary")
                .condition(lessThan(thisVariable.getField(selectedPositionsField).length(), positionCount))
                .ifTrue(thisVariable.setField(selectedPositionsField, newArray(type(boolean[].class), positionCount))));

        Variable selectedPositions = scope.declareVariable("selectedPositions", body, thisVariable.getField(selectedPositionsField));
        Variable position = scope.declareVariable(int.class, "position");
        body.append(new ForLoop()
                .initialize(position.set(constantInt(0)))
                .condition(lessThan(position, positionCount))
                .update(position.increment())
                .body(selectedPositions.setElement(position, thisVariable.invoke("filter", boolean.class, properties, page, position))));

        body.append(invokeStatic(
                PageFilter.class,
                "positionsArrayToSelectedPositions",
                SelectedPositions.class,
                selectedPositions,
                positionCount)
                .ret());

        return method;
    }

    private MethodDefinition generateFilterMethod(
            SqlFunctionProperties sqlFunctionProperties,
            ClassDefinition classDefinition,
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            RowExpression filter)
    {
        Parameter properties = arg("properties", SqlFunctionProperties.class);
        Parameter page = arg("page", Page.class);
        Parameter position = arg("position", int.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "filter",
                type(boolean.class),
                ImmutableList.<Parameter>builder()
                        .add(properties)
                        .add(page)
                        .add(position)
                        .build());

        method.comment("Filter: %s", filter.toString());

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        declareBlockVariables(ImmutableList.of(filter), page, scope, body);

        Variable wasNullVariable = scope.declareVariable("wasNull", body, constantFalse());
        RowExpressionCompiler compiler = new RowExpressionCompiler(
                classDefinition,
                callSiteBinder,
                cachedInstanceBinder,
                fieldReferenceCompiler(callSiteBinder),
                metadata,
                sqlFunctionProperties,
                compiledLambdaMap);

        Variable result = scope.declareVariable(boolean.class, "result");
        body.append(compiler.compile(filter, scope, Optional.empty()))
                // store result so we can check for null
                .putVariable(result)
                .append(and(not(wasNullVariable), result).ret());
        return method;
    }

    private static void generateConstructor(
            ClassDefinition classDefinition,
            CachedInstanceBinder cachedInstanceBinder,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            Consumer<MethodDefinition> additionalStatements)
    {
        MethodDefinition constructorDefinition = classDefinition.declareConstructor(a(PUBLIC));

        BytecodeBlock body = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();

        body.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);

        additionalStatements.accept(constructorDefinition);

        cachedInstanceBinder.generateInitializations(thisVariable, body);
        body.ret();
    }

    private static void declareBlockVariables(List<RowExpression> expressions, Parameter page, Scope scope, BytecodeBlock body)
    {
        for (int channel : getInputChannels(expressions)) {
            scope.declareVariable("block_" + channel, body, page.invoke("getBlock", Block.class, constantInt(channel)));
        }
    }

    private static List<Integer> getInputChannels(Iterable<RowExpression> expressions)
    {
        TreeSet<Integer> channels = new TreeSet<>();
        for (RowExpression expression : Expressions.subExpressions(expressions)) {
            if (expression instanceof InputReferenceExpression) {
                channels.add(((InputReferenceExpression) expression).getField());
            }
        }
        return ImmutableList.copyOf(channels);
    }

    private static List<Integer> getInputChannels(RowExpression expression)
    {
        return getInputChannels(ImmutableList.of(expression));
    }

    private static List<Parameter> toBlockParameters(List<Integer> inputChannels)
    {
        ImmutableList.Builder<Parameter> parameters = ImmutableList.builder();
        for (int channel : inputChannels) {
            parameters.add(arg("block_" + channel, Block.class));
        }
        return parameters.build();
    }

    private static RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler(CallSiteBinder callSiteBinder)
    {
        return new InputReferenceCompiler(
                (scope, field) -> scope.getVariable("block_" + field),
                (scope, field) -> scope.getVariable("position"),
                callSiteBinder);
    }

    private static final class CacheKey
    {
        private final SqlFunctionProperties sqlFunctionProperties;
        private final List<RowExpression> rowExpressions;

        private CacheKey(SqlFunctionProperties sqlFunctionProperties, List<RowExpression> rowExpressions)
        {
            requireNonNull(rowExpressions, "rowExpressions is null");
            checkArgument(rowExpressions.size() >= 1, "Expect at least one RowExpression");
            this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties, "sqlFunctionProperties is null");
            this.rowExpressions = ImmutableList.copyOf(rowExpressions);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey that = (CacheKey) o;
            return Objects.equals(sqlFunctionProperties, that.sqlFunctionProperties) &&
                    Objects.equals(rowExpressions, that.rowExpressions);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sqlFunctionProperties, rowExpressions);
        }
    }
}
