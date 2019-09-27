/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.runtime.SqlFunctions.toLong;
import static org.apache.calcite.runtime.SqlFunctions.tumbleWindowEnd;
import static org.apache.calcite.runtime.SqlFunctions.tumbleWindowStart;

/** Implementation of {@link org.apache.calcite.rel.core.TableFunctionScan} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableFunctionScan extends TableFunctionScan
    implements EnumerableRel {

  public EnumerableTableFunctionScan(RelOptCluster cluster,
      RelTraitSet traits, List<RelNode> inputs, Type elementType,
      RelDataType rowType, RexNode call,
      Set<RelColumnMapping> columnMappings) {
    super(cluster, traits, inputs, call, elementType, rowType,
        columnMappings);
  }

  @Override public EnumerableTableFunctionScan copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      RexNode rexCall,
      Type elementType,
      RelDataType rowType,
      Set<RelColumnMapping> columnMappings) {
    return new EnumerableTableFunctionScan(getCluster(), traitSet, inputs,
        elementType, rowType, rexCall, columnMappings);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    if (getCall().getKind() == SqlKind.TUMBLE) {
      return tableValuedFunctionWindowingImplement(implementor, pref);
    } else {
      return defaultTableValuedFunctionImplement(implementor, pref);
    }
  }

  private boolean isQueryable() {
    if (!(getCall() instanceof RexCall)) {
      return false;
    }
    final RexCall call = (RexCall) getCall();
    if (!(call.getOperator() instanceof SqlUserDefinedTableFunction)) {
      return false;
    }
    final SqlUserDefinedTableFunction udtf =
        (SqlUserDefinedTableFunction) call.getOperator();
    if (!(udtf.getFunction() instanceof TableFunctionImpl)) {
      return false;
    }
    final TableFunctionImpl tableFunction =
        (TableFunctionImpl) udtf.getFunction();
    final Method method = tableFunction.method;
    return QueryableTable.class.isAssignableFrom(method.getReturnType());
  }

  private Result defaultTableValuedFunctionImplement(
      EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder bb = new BlockBuilder();
    // Non-array user-specified types are not supported yet
    final JavaRowFormat format;
    if (getElementType() == null) {
      format = JavaRowFormat.ARRAY;
    } else if (rowType.getFieldCount() == 1 && isQueryable()) {
      format = JavaRowFormat.SCALAR;
    } else if (getElementType() instanceof Class
        && Object[].class.isAssignableFrom((Class) getElementType())) {
      format = JavaRowFormat.ARRAY;
    } else {
      format = JavaRowFormat.CUSTOM;
    }
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), format,
            false);
    RexToLixTranslator t = RexToLixTranslator.forAggregation(
        (JavaTypeFactory) getCluster().getTypeFactory(), bb, null,
        implementor.getConformance());
    t = t.setCorrelates(implementor.allCorrelateVariables);
    bb.add(Expressions.return_(null, t.translate(getCall())));
    return implementor.result(physType, bb.toBlock());
  }

  private Result tableValuedFunctionWindowingImplement(
      EnumerableRelImplementor implementor, Prefer pref) {
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInputs().get(0);
    final Result result =
        implementor.visitChild(this, 0, child, pref);
    final PhysType physType = PhysTypeImpl.of(
        typeFactory, getRowType(), pref.prefer(result.format));
    Type inputJavaType = result.physType.getJavaRowType();

    ParameterExpression inputEnumerator =
        Expressions.parameter(
            Types.of(Enumerator.class, inputJavaType), "inputEnumerator");
    Expression input =
        EnumUtils.convert(
            Expressions.call(
                inputEnumerator,
                BuiltInMethod.ENUMERATOR_CURRENT.method),
            inputJavaType);

    final SqlConformance conformance =
        (SqlConformance) implementor.map.getOrDefault("_conformance",
            SqlConformanceEnum.DEFAULT);

    List<Expression> expressions =
        RexToLixTranslator.translateTableFunction(
            typeFactory,
            conformance,
            builder,
            DataContext.ROOT,
            new RexToLixTranslator.InputGetterImpl(
                Collections.singletonList(
                    Pair.of(input, result.physType))),
            (RexCall) getCall());

    final Expression inputEnumerable = builder.append(
        "inputEnumerable", result.block, false);

    builder.add(
        Expressions.call(
            Types.lookupMethod(
                this.getClass(), "tumbling", Enumerator.class, int.class, long.class),
            Expressions.list(
                Expressions.call(inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method),
                expressions.get(0),
                expressions.get(1))));

    return implementor.result(physType, builder.toBlock());
  }

  public static Enumerable<Object[]> tumbling(Enumerator<Object[]> inputEnumerator,
      int indexOfWatermarkedColumn,
      long intervalSize) {
    return new AbstractEnumerable<Object[]>() {
      @Override public Enumerator<Object[]> enumerator() {
        return new TumbleEnumerator(inputEnumerator, indexOfWatermarkedColumn, intervalSize);
      }
    };
  }

  /**
   * TumbleEnumerator applies tumbling on each element from the input enumerator and produces
   * exactly one element for each input element.
   */
  private static class TumbleEnumerator implements Enumerator<Object[]> {
    private final Enumerator<Object[]> inputEnumerator;
    private final int indexOfWatermarkedColumn;
    private final long intervalSize;

    TumbleEnumerator(Enumerator<Object[]> inputEnumerator,
        int indexOfWatermarkedColumn, long intervalSize) {
      this.inputEnumerator = inputEnumerator;
      this.indexOfWatermarkedColumn = indexOfWatermarkedColumn;
      this.intervalSize = intervalSize;
    }

    public Object[] current() {
      Object[] current = inputEnumerator.current();
      Object[] ret = new Object[current.length + 2];
      System.arraycopy(current, 0, ret, 0, current.length);
      ret[current.length] =
          tumbleWindowStart(toLong(current[indexOfWatermarkedColumn]), intervalSize);
      ret[current.length + 1] =
          tumbleWindowEnd(toLong(current[indexOfWatermarkedColumn]), intervalSize);
      return ret;
    }

    public boolean moveNext() {
      return inputEnumerator.moveNext();
    }

    public void reset() {
      inputEnumerator.reset();
    }

    public void close() {
    }
  }
}
