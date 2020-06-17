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
package org.apache.calcite.benchmarks;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Digest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A benchmark of the digest {@link org.apache.calcite.plan.Digest}.
 *
 * <p>The benchmark emphasizes on the memory usage and performance.
 */
@Fork(value = 1, jvmArgsPrepend = "-Xmx1024m")
public class DigestBenchmark {

  private static class StringDigest {
    final String digest;

    private StringDigest(String digest) {
      this.digest = digest;
    }

    @Override public boolean equals(Object o) {
      return o == this
          || o instanceof StringDigest
          && ((StringDigest) o).digest.equals(digest);
    }

    @Override public int hashCode() {
      return digest.hashCode();
    }
  }

  /**
   * State object for the benchmarks.
   */
  @State(Scope.Thread)
  public static class DigestState {

    /**
     * The number of joins for each generated query.
     */
    @Param({"1", "10", "20"})
    int joins;

    /**
     * The number of disjunctions for each generated query.
     */
    @Param({"1", "10", "100"})
    int whereClauseDisjunctions;

    @Param({"true"})
    boolean isStringDigest;

    List<RelNode> rels;

    Map<Pair<StringDigest, List<RelDataType>>, RelNode> oldDigestToRelMap;

    Map<Digest, RelNode> newDigestToRelMap;

    private void initializeState(RelNode query) {
      RelVisitor visitor = new RelVisitor() {
        @Override public void visit(RelNode node, int ordinal, RelNode parent) {
          rels.add(node);
          putToMap(node);
          super.visit(node, ordinal, parent);
        }
      };
      visitor.go(query);
    }

    private void putToMap(RelNode node) {
      if (isStringDigest) {
        oldDigestToRelMap.put(
            Pair.of(
                new StringDigest(node.toString()),
                Pair.right(node.getRowType().getFieldList())),
            node);
      } else {
        newDigestToRelMap.put(node.getDigest(), node);
      }
    }

    @Setup(Level.Invocation)
    public void setUp() {
      rels = new ArrayList<>();
      oldDigestToRelMap = new HashMap<>();
      newDigestToRelMap = new HashMap<>();

      VolcanoPlanner planner = new VolcanoPlanner();

      RelDataTypeFactory typeFactory =
          new JavaTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
      RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

      RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
      // Generates queries of the following form depending on the configuration parameters.
      // SELECT `t`.`name`
      // FROM (VALUES  (1, 'name0')) AS `t` (`id`, `name`)
      // INNER JOIN (VALUES  (1, 'name1')) AS `t` (`id`, `name`) AS `t0` ON `t`.`id` = `t0`.`id`
      // INNER JOIN (VALUES  (2, 'name2')) AS `t` (`id`, `name`) AS `t1` ON `t`.`id` = `t1`.`id`
      // INNER JOIN (VALUES  (3, 'name3')) AS `t` (`id`, `name`) AS `t2` ON `t`.`id` = `t2`.`id`
      // INNER JOIN ...
      // WHERE
      //  `t`.`name` = 'name0' OR
      //  `t`.`name` = 'name1' OR
      //  `t`.`name` = 'name2' OR
      //  ...
      //  OR `t`.`id` = 1
      relBuilder.values(new String[]{"id", "name"}, 1, "name0");
      for (int j = 1; j <= joins; j++) {
        relBuilder
            .values(new String[]{"id", "name"}, j, "name" + j)
            .join(JoinRelType.INNER, "id");
      }

      List<RexNode> disjunctions = new ArrayList<>();
      for (int j = 0; j < whereClauseDisjunctions; j++) {
        disjunctions.add(
            relBuilder.equals(
                relBuilder.field("name"),
                relBuilder.literal("name" + j)));
      }
      disjunctions.add(
          relBuilder.equals(
              relBuilder.field("id"),
              relBuilder.literal(1)));
      RelNode query =
          relBuilder
              .filter(relBuilder.or(disjunctions))
              .project(relBuilder.field("name"))
              .build();

      initializeState(query);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void getRelFromDigestToRelMap(DigestState state) {
    for (RelNode rel : state.rels) {
      if (state.isStringDigest) {
        state.oldDigestToRelMap.get(
            Pair.of(
                new StringDigest(rel.toString()),
                Pair.right(rel.getRowType().getFieldList())));
      } else {
        state.newDigestToRelMap.get(rel.getDigest());
      }
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(DigestBenchmark.class.getName())
        .detectJvmArgs()
        .addProfiler(MaxMemoryProfiler.class)
        .build();

    new Runner(opt).run();
  }
}
