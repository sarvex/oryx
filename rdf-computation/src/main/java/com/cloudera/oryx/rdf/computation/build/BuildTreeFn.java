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

package com.cloudera.oryx.rdf.computation.build;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.util.FastMath;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.random.RandomManager;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.computation.common.fn.OryxReduceDoFn;
import com.cloudera.oryx.rdf.common.eval.Evaluation;
import com.cloudera.oryx.rdf.common.example.CategoricalFeature;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;

/**
 * @author Sean Owen
 */
public final class BuildTreeFn extends OryxReduceDoFn<Integer, Iterable<String>, String> {

  private static final Logger log = LoggerFactory.getLogger(BuildTreeFn.class);

  private static final CharMatcher NEWLINES = CharMatcher.anyOf("\n\r");

  private int numLocalTrees;
  private int trainingFoldsPerTree;
  private final RandomDataGenerator random = new RandomDataGenerator(RandomManager.getRandom());

  @Override
  public void initialize() {
    super.initialize();
    int numReducers = getContext().getNumReduceTasks();
    log.info("{} reducers", numReducers);

    Config config = ConfigUtils.getDefaultConfig();
    int numTrees = config.getInt("model.num-trees");
    // Bump this up to as least 2x reducers
    numTrees = FastMath.max(numTrees, 2 * numReducers);
    // Make it a multiple of # reducers
    while ((numTrees % numReducers) != 0) {
      numTrees++;
    }
    log.info("{} total trees", numTrees);

    Preconditions.checkArgument(numTrees % numReducers == 0, "numLocalTrees not a multiples of numReducers");
    numLocalTrees = numTrees / numReducers;
    Preconditions.checkArgument(numLocalTrees > 1, "Not building multiple trees per reducer");
    log.info("Building {} trees locally", numLocalTrees);

    double sampleRate = config.getDouble("model.sample-rate");
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);

    // Arbitrarily use about 90% for training, locally
    trainingFoldsPerTree = FastMath.min(numLocalTrees - 1, (int) (0.9 * numLocalTrees));

    log.info("{} training folds per tree", trainingFoldsPerTree);
  }

  @Override
  public void process(Pair<Integer,Iterable<String>> input, Emitter<String> emitter) {

    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int numFeaturesAndTarget = inboundSettings.getColumnNames().size();
    Integer targetColumn = inboundSettings.getTargetColumn();
    Preconditions.checkNotNull(targetColumn, "No target-column specified");

    List<Example> allExamples = Lists.newArrayList();
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping = Maps.newHashMap();

    log.info("Reading input");
    for (String line : input.second()) {
      String[] tokens = DelimitedDataUtils.decode(line);
      Preconditions.checkArgument(tokens.length == numFeaturesAndTarget,
                                  "Wrong number of tokens in line: %s", line);
      Feature target = null;
      Feature[] features = new Feature[numFeaturesAndTarget]; // Too big by 1 but makes math easier
      for (int col = 0; col < numFeaturesAndTarget; col++) {
        if (col == targetColumn) {
          target = buildFeature(col, tokens[col], inboundSettings, columnToCategoryNameToIDMapping);
          features[col] = IgnoredFeature.INSTANCE;
        } else {
          features[col] = buildFeature(col, tokens[col], inboundSettings, columnToCategoryNameToIDMapping);
        }
      }
      Preconditions.checkNotNull(target, "No target value in line %s", line);
      allExamples.add(new Example(target, features));
    }

    if (allExamples.isEmpty()) {
      return;
    }

    log.info("Read {} examples", allExamples.size());

    for (int treeID = 0; treeID < numLocalTrees; treeID++) {
      List<Example> trainingExamples = Lists.newArrayList();
      List<Example> cvExamples = Lists.newArrayList();
      for (Example example : allExamples) {
        if (random.nextInt(0, numLocalTrees - 1) < trainingFoldsPerTree) {
          trainingExamples.add(example);
        } else {
          cvExamples.add(example);
        }
      }

      log.info("Tree {}: {} training examples", treeID, trainingExamples.size());
      log.info("Tree {}: {} CV examples", treeID, cvExamples.size());
      Preconditions.checkState(!trainingExamples.isEmpty(), "No training examples sampled?");
      Preconditions.checkState(!cvExamples.isEmpty(), "No CV examples sampled?");

      progress(); // Helps prevent timeouts
      DecisionTree tree = DecisionTree.fromExamplesWithDefault(trainingExamples);
      progress(); // Helps prevent timeouts
      log.info("Built tree {}", treeID);
      ExampleSet cvSet = new ExampleSet(cvExamples);
      double[] weightEval = Evaluation.evaluateToWeight(tree, cvSet);
      double weight = weightEval[0];
      double eval = weightEval[1];
      double[] featureImportances = tree.featureImportance(cvSet);
      progress(); // Helps prevent timeouts
      log.info("Evaluated tree {}: {}", treeID, eval);
      log.info("Feature importances: {}", featureImportances);

      DecisionForest singletonForest = new DecisionForest(
          new DecisionTree[] { tree }, new double[] { weight }, featureImportances);

      String pmmlFileContents;
      try  {
        StringWriter treePMML = new StringWriter();
        DecisionForestPMML.write(treePMML, singletonForest, columnToCategoryNameToIDMapping);
        pmmlFileContents = NEWLINES.removeFrom(treePMML.toString());
      } catch (IOException ioe) {
        throw new IllegalStateException(ioe);
      }

      log.info("Emitting tree {}", treeID);
      emitter.emit(pmmlFileContents);
    }

  }

  private static Feature buildFeature(int columnNumber,
                                      String token,
                                      InboundSettings inboundSettings,
                                      Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    if (token == null || token.isEmpty()) {
      return null;
    }
    if (inboundSettings.isNumeric(columnNumber)) {
      return NumericFeature.forValue(Float.parseFloat(token));
    }
    if (inboundSettings.isCategorical(columnNumber)) {
      return CategoricalFeature.forValue(categoricalFromString(columnNumber, token, columnToCategoryNameToIDMapping));
    }
    return IgnoredFeature.INSTANCE;
  }

  private static int categoricalFromString(int columnNumber,
                                           String value,
                                           Map<Integer, BiMap<String, Integer>> columnToCategoryNameToIDMapping) {
    BiMap<String,Integer> categoryNameToID = columnToCategoryNameToIDMapping.get(columnNumber);
    if (categoryNameToID == null) {
      categoryNameToID = HashBiMap.create();
      columnToCategoryNameToIDMapping.put(columnNumber, categoryNameToID);
    }
    Integer mapped = categoryNameToID.get(value);
    if (mapped != null) {
      return mapped;
    }
    int newCategory = categoryNameToID.size();
    categoryNameToID.put(value, newCategory);
    return newCategory;
  }

}
