/*
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.oryx.als.computation.merge;

import java.io.IOException;

import com.cloudera.oryx.als.common.NumericIDValue;
import com.cloudera.oryx.als.computation.types.ALSTypes;
import com.google.common.base.Preconditions;
import org.apache.crunch.GroupingOptions;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.join.JoinUtils;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.avro.Avros;

import com.cloudera.oryx.als.computation.ALSJobStep;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;

/**
 * @author Sean Owen
 */
public final class MergeNewOldStep extends ALSJobStep {

  private static final PTableType<Pair<Long, Integer>, NumericIDValue> JOIN_TYPE = Avros.tableOf(
      Avros.pairs(ALSTypes.LONGS, ALSTypes.INTS),
      ALSTypes.IDVALUE);

  @Override
  protected MRPipeline createPipeline() throws IOException {

    JobStepConfig jobConfig = getConfig();

    String instanceDir = jobConfig.getInstanceDir();
    int generationID = jobConfig.getGenerationID();
    int lastGenerationID = jobConfig.getLastGenerationID();

    String outputKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "input/";
    if (!validOutputPath(outputKey)) {
      return null;
    }

    MRPipeline p = createBasicPipeline(MergeNewOldValuesFn.class);

    String trainKey = Namespaces.getInstanceGenerationPrefix(instanceDir, generationID) + "train/";

    PCollection<Pair<Long, NumericIDValue>> parsed = p.read(textInput(trainKey))
        .parallelDo("trainParse", new DelimitedInputParseFn(),
            Avros.pairs(ALSTypes.LONGS, ALSTypes.IDVALUE));

    PTable<Pair<Long, Integer>, NumericIDValue> train = parsed.parallelDo("train", new InboundJoinFn(), JOIN_TYPE);

    float decayFactor = (float) ConfigUtils.getDefaultConfig().getDouble("model.decay.factor");

    // Only join past data if it exists, and, decay factor is not 0
    if (lastGenerationID >= 0 && decayFactor > 0.0f) {
      String inputPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "input/";
      Store store = Store.get();
      Preconditions.checkState(store.exists(inputPrefix, false), "Input path does not exist: %s", inputPrefix);
      PTable<Pair<Long, Integer>, NumericIDValue> joinInputBefore = p.read(input(inputPrefix, ALSTypes.VALUE_MATRIX))
          .parallelDo("lastGenerationInput", new JoinBeforeMapFn(), JOIN_TYPE);
      train = train.union(joinInputBefore);

      String testPrefix = Namespaces.getInstanceGenerationPrefix(instanceDir, lastGenerationID) + "test/";
      if (store.exists(testPrefix, false)) {
        // Held out test data last time; incorporate now
        PTable<Pair<Long, Integer>, NumericIDValue> joinTestSetBefore =
            p.read(textInput(testPrefix))
                .parallelDo("testParse", new DelimitedInputParseFn(), Avros.pairs(ALSTypes.LONGS, ALSTypes.IDVALUE))
                .parallelDo("lastGenerationTest", new JoinBeforeMapFn(), JOIN_TYPE);
        train = train.union(joinTestSetBefore);
      }
    }

    GroupingOptions groupingOptions = GroupingOptions.builder()
        .partitionerClass(JoinUtils.getPartitionerClass(train.getTypeFamily()))
        .numReducers(getNumReducers())
        .build();

    train
        .groupByKey(groupingOptions)
        .parallelDo(new MergeNewOldValuesFn(), ALSTypes.VALUE_MATRIX)
        .write(output(outputKey));
    return p;
  }

  public static void main(String[] args) throws Exception {
    run(new MergeNewOldStep(), args);
  }

}
