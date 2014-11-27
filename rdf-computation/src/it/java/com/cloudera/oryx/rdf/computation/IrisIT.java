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

package com.cloudera.oryx.rdf.computation;

import com.google.common.collect.BiMap;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import org.apache.commons.math3.util.Pair;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.NumericFeature;
import com.cloudera.oryx.rdf.common.pmml.DecisionForestPMML;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;
import com.cloudera.oryx.rdf.computation.local.RDFLocalGenerationRunner;

/**
 * Tests the random decision forest classifier on the classic
 * <a href="http://archive.ics.uci.edu/ml/datasets/Iris">Iris data set</a>. This contains
 * a few numeric features and a categorical target.
 *
 * @author Sean Owen
 */
public final class IrisIT extends AbstractComputationIT {

  private static final Logger log = LoggerFactory.getLogger(IrisIT.class);

  @Override
  protected File getTestDataPath() {
    return getResourceAsFile("iris");
  }

  @Test
  public void testPMMLOutput() throws Exception {
    // It's not clear this will actually be deterministic but it will probably be for our purposes
    new RDFLocalGenerationRunner().call();
    File pmmlFile = new File(TEST_TEMP_BASE_DIR, "00000/model.pmml.gz");

    log.info("PMML:\n{}", CharStreams.toString(IOUtils.openReaderMaybeDecompressing(pmmlFile)));

    Pair<DecisionForest,Map<Integer,BiMap<String,Integer>>> forestAndMapping = DecisionForestPMML.read(pmmlFile);
    DecisionForest forest = forestAndMapping.getFirst();
    Map<Integer,BiMap<String,Integer>> categoryValueMapping = forestAndMapping.getSecond();
    Map<String,Integer> targetCategoryValueMapping = categoryValueMapping.get(4);

    log.info("{}", forest);

    double[] importances = forest.getFeatureImportances();
    for (double d : importances) {
      assertTrue(d >= 0.0);
      assertTrue(d <= 1.0);
    }
    // petal length ought to be most predictive
    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int petalLengthColumn = settings.getColumnNames().indexOf("petal length");
    int petalWidthColumn = settings.getColumnNames().indexOf("petal width");
    double mostImportant = Doubles.max(importances);
    assertTrue(importances[petalLengthColumn] == mostImportant ||
                   importances[petalWidthColumn] == mostImportant);


    DecisionTree[] trees = forest.getTrees();
    assertEquals(2, trees.length);

    Feature[] features = {
        //5.9,3.0,5.1,1.8,Iris-virginica
        NumericFeature.forValue(5.9f),
        NumericFeature.forValue(3.0f),
        NumericFeature.forValue(5.1f),
        NumericFeature.forValue(1.8f),
    };
    Example example = new Example(null, features);
    CategoricalPrediction prediction = (CategoricalPrediction) forest.classify(example);
    int expectedCategory = targetCategoryValueMapping.get("Iris-virginica");
    assertEquals(expectedCategory, prediction.getMostProbableCategoryID());
    assertEquals(prediction.getCategoryProbabilities()[expectedCategory],
                 Floats.max(prediction.getCategoryProbabilities()));
  }

}
