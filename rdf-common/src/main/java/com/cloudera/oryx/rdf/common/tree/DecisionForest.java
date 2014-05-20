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

package com.cloudera.oryx.rdf.common.tree;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.math.IntMath;
import com.typesafe.config.Config;
import org.apache.commons.math3.util.FastMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.cloudera.oryx.common.iterator.ArrayIterator;
import com.cloudera.oryx.common.parallel.ExecutorUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.rdf.common.eval.Evaluation;
import com.cloudera.oryx.rdf.common.eval.WeightedPrediction;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.ExampleSet;
import com.cloudera.oryx.rdf.common.rule.Prediction;

/**
 * An ensemble classifier based on many {@link DecisionTree}s.
 *
 * @author Sean Owen
 * @see DecisionTree
 */
public final class DecisionForest implements Iterable<DecisionTree>, TreeBasedClassifier {

  private static final Logger log = LoggerFactory.getLogger(DecisionForest.class);

  private final DecisionTree[] trees;
  private final double[] weights;
  private final double[] evaluations;
  private final double[] featureImportances;

  public static DecisionForest fromExamplesWithDefault(List<Example> examples) {
    Config config = ConfigUtils.getDefaultConfig();
    int numTrees = config.getInt("model.num-trees");
    double fractionOfFeaturesToTry = config.getDouble("model.fraction-features-to-try");
    int minNodeSize = config.getInt("model.min-node-size");
    double minInfoGainNats = config.getDouble("model.min-info-gain-nats");
    int suggestedMaxSplitCandidates = config.getInt("model.max-split-candidates");
    int maxDepth = config.getInt("model.max-depth");
    double sampleRate = config.getDouble("model.sample-rate");
    ExampleSet exampleSet = new ExampleSet(examples);
    return new DecisionForest(numTrees,
                              fractionOfFeaturesToTry,
                              minNodeSize,
                              minInfoGainNats,
                              suggestedMaxSplitCandidates,
                              maxDepth,
                              sampleRate,
                              exampleSet);
  }

  public DecisionForest(final int numTrees,
                        double fractionOfFeaturesToTry,
                        final int minNodeSize,
                        final double minInfoGainNats,
                        final int suggestedMaxSplitCandidates,
                        final int maxDepth,
                        final double sampleRate,
                        final ExampleSet examples) {
    Preconditions.checkArgument(numTrees > 1);
    final int numFeatures = examples.getNumFeatures();
    Preconditions.checkArgument(fractionOfFeaturesToTry > 0.0 && fractionOfFeaturesToTry <= 1.0);
    final int featuresToTry = FastMath.max(1, (int) (fractionOfFeaturesToTry * numFeatures));
    Preconditions.checkArgument(numFeatures >= 1);
    Preconditions.checkArgument(minNodeSize >= 1);
    Preconditions.checkArgument(minInfoGainNats >= 0.0);
    Preconditions.checkArgument(suggestedMaxSplitCandidates >= 1);
    Preconditions.checkArgument(maxDepth >= 1);
    Preconditions.checkArgument(sampleRate > 0.0 && sampleRate <= 1.0);

    weights = new double[numTrees];
    Arrays.fill(weights, 1.0);
    evaluations = new double[numTrees];
    Arrays.fill(evaluations, Double.NaN);
    final double[][] perTreeFeatureImportances = new double[numTrees][];

    // Going to set an arbitrary upper bound on the training size of about 90%
    int maxFolds = FastMath.min(numTrees - 1, (int) (0.9 * numTrees));
    // Going to set an arbitrary lower bound on the CV size of about 10%
    int minFolds = FastMath.max(1, (int) (0.1 * numTrees));
    final int folds = FastMath.min(maxFolds, FastMath.max(minFolds, (int) (sampleRate * numTrees)));

    trees = new DecisionTree[numTrees];

    ExecutorService executor = Executors.newFixedThreadPool(determineParallelism(trees.length));
    try {
      Collection<Future<Object>> futures = Lists.newArrayListWithCapacity(trees.length);
      for (int i = 0; i < numTrees; i++) {
        final int treeID = i;
        futures.add(executor.submit(new Callable<Object>() {
          @Override
          public Void call() throws Exception {
            Collection<Example> allExamples = examples.getExamples();
            int totalExamples = allExamples.size();
            int expectedTrainingSize = (int) (totalExamples * sampleRate);
            int expectedCVSize = totalExamples - expectedTrainingSize;
            List<Example> trainingExamples = Lists.newArrayListWithExpectedSize(expectedTrainingSize);
            List<Example> cvExamples = Lists.newArrayListWithExpectedSize(expectedCVSize);
            for (Example example : allExamples) {
              if (IntMath.mod(IntMath.mod(example.hashCode(), numTrees) - treeID, numTrees) < folds) {
                trainingExamples.add(example);
              } else {
                cvExamples.add(example);
              }
            }

            Preconditions.checkState(!trainingExamples.isEmpty(), "No training examples sampled?");
            Preconditions.checkState(!cvExamples.isEmpty(), "No CV examples sampled?");

            log.info("Starting tree {}", treeID);
            trees[treeID] = new DecisionTree(numFeatures,
                                             featuresToTry,
                                             minNodeSize,
                                             minInfoGainNats,
                                             suggestedMaxSplitCandidates,
                                             maxDepth,
                                             examples.subset(trainingExamples));
            log.info("Finished tree {}", treeID);
            ExampleSet cvExampleSet = examples.subset(cvExamples);
            double[] weightEval = Evaluation.evaluateToWeight(trees[treeID], cvExampleSet);
            weights[treeID] = weightEval[0];
            evaluations[treeID] = weightEval[1];
            perTreeFeatureImportances[treeID] = trees[treeID].featureImportance(cvExampleSet);
            log.info("Tree {} eval: {}", treeID, weightEval[1]);
            return null;
          }
        }));
      }
      ExecutorUtils.getResults(futures);
    } finally {
      ExecutorUtils.shutdownNowAndAwait(executor);
    }

    featureImportances = new double[numFeatures];
    for (double[] perTreeFeatureImportance : perTreeFeatureImportances) {
      for (int i = 0; i < numFeatures; i++) {
        featureImportances[i] += perTreeFeatureImportance[i];
      }
    }
    for (int i = 0; i < numFeatures; i++) {
      featureImportances[i] /= numTrees;
    }
  }

  public DecisionForest(DecisionTree[] trees, double[] weights, double[] featureImportances) {
    this.trees = trees;
    this.weights = weights;
    this.evaluations = new double[weights.length];
    this.featureImportances = featureImportances;
  }

  @Override
  public Iterator<DecisionTree> iterator() {
    return ArrayIterator.forArray(trees);
  }

  /**
   * @return {@link DecisionTree}s in the ensemble forest
   */
  public DecisionTree[] getTrees() {
    return trees;
  }

  public double[] getWeights() {
    return weights;
  }

  public double[] getEvaluations() {
    return evaluations;
  }

  public double[] getFeatureImportances() {
    return featureImportances;
  }
  
  @Override
  public Prediction classify(Example test) {
    return WeightedPrediction.voteOnFeature(
        Lists.transform(Arrays.asList(trees), new TreeToPredictionFunction(test)), weights);
  }

  @Override
  public void update(Example train) {
    for (TreeBasedClassifier tree : trees) {
      tree.update(train);
    }
  }

  private static int determineParallelism(int numTrees) {
    int numCores = ExecutorUtils.getParallelism();
    if (numCores >= numTrees) {
      return numTrees;
    }
    // Try to round up threads so trees is a multiple of it
    int numThreads = numCores;
    while (numTrees % numThreads != 0 && numThreads < 2 * numCores) {
      numThreads++;
    }
    return numThreads;
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    for (DecisionTree tree : trees) {
      result.append(tree).append('\n');
    }
    return result.toString();
  }

  private static final class TreeToPredictionFunction implements Function<DecisionTree, Prediction> {
    private final Example test;
    TreeToPredictionFunction(Example test) {
      this.test = test;
    }
    @Override
    public Prediction apply(DecisionTree tree) {
      return tree.classify(test);
    }
  }
}
