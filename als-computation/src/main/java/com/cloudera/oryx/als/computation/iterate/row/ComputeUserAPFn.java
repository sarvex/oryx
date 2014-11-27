/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
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

package com.cloudera.oryx.als.computation.iterate.row;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import com.cloudera.oryx.als.common.StringLongMapping;
import com.cloudera.oryx.common.collection.LongObjectMap;
import com.cloudera.oryx.common.collection.LongSet;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.math.SimpleVectorMath;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.fn.OryxDoFn;

public final class ComputeUserAPFn extends OryxDoFn<Pair<Long, float[]>, Double> {

  private static final Logger log = LoggerFactory.getLogger(ComputeUserAPFn.class);

  private final YState yState;
  private LongObjectMap<LongSet> testData;

  public ComputeUserAPFn(YState yState) {
    this.yState = yState;
  }

  @Override
  public void initialize() {
    super.initialize();
    Store store = Store.get();
    testData = new LongObjectMap<LongSet>();
    String prefix = getConfiguration().get(RowStep.MAP_KEY);
    try {
      for (String filePrefix : store.list(prefix, true)) {
        for (CharSequence line : new FileLineIterable(store.readFrom(filePrefix))) {
          String[] columns = DelimitedDataUtils.decode(line);
          long userID = StringLongMapping.toLong(columns[0]);
          long itemID = StringLongMapping.toLong(columns[1]);
          LongSet itemIDs = testData.get(userID);
          if (itemIDs == null) {
            itemIDs = new LongSet();
            testData.put(userID, itemIDs);
          }
          itemIDs.add(itemID);
        }
      }
    } catch (IOException ioe) {
      throw new CrunchRuntimeException(ioe);
    }
  }

  @Override
  public void process(Pair<Long, float[]> input, Emitter<Double> emitter) {

    LongSet ids = testData.get(input.first());
    if (ids == null || ids.isEmpty()) {
      return;
    }

    float[] userVector = input.second();
    LongObjectMap<float[]> Y = yState.getY();
    long[] itemIDs = ids.toArray();

    double[] scores = new double[itemIDs.length];
    for (int i = 0; i < itemIDs.length; i++) {
      long itemID = itemIDs[i];
      float[] itemVector = Y.get(itemID);
      if (itemVector == null) {
        continue;
      }
      scores[i] = SimpleVectorMath.dot(userVector, itemVector);
    }

    int[] rank = new int[itemIDs.length];

    for (LongObjectMap.MapEntry<float[]> entry : Y.entrySet()) {
      double score = SimpleVectorMath.dot(userVector, entry.getValue());
      for (int i = 0; i < itemIDs.length; i++) {
        if (score > scores[i]) {
          rank[i]++;
        }
      }
    }

    Arrays.sort(rank);

    Mean precision = new Mean();
    double totalPrecisionTimesRelevance = 0.0;
    for (int i = 0; i < rank.length; i++) {
      int relevantRetrieved = i + 1;
      int precisionAt = rank[i] + 1;
      precision.increment((double) relevantRetrieved / precisionAt);
      totalPrecisionTimesRelevance += precision.getResult();
    }
    double averagePrecision = totalPrecisionTimesRelevance / rank.length;

    log.info("Average precision: {}", averagePrecision);

    emitter.emit(averagePrecision);
  }

}
