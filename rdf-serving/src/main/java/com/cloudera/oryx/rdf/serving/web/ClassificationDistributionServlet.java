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

package com.cloudera.oryx.rdf.serving.web;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.Example;
import com.cloudera.oryx.rdf.common.example.Feature;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.example.IgnoredFeature;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.Prediction;
import com.cloudera.oryx.rdf.common.tree.TreeBasedClassifier;
import com.cloudera.oryx.rdf.serving.generation.Generation;

/**
 * <p>Like {@link ClassifyServlet}, but responds at endpoint {@code /classificationDistribution}.
 * This returns not just the most probable category, but all categories and their associated probability.
 * The output is "category,probability", one per line for each category value.
 * Returns an error for models with numeric target feature. That is, this is only supported
 * for classification problems, not regression.</p>
 *
 * @author Sean Owen
 */
public final class ClassificationDistributionServlet extends AbstractRDFServlet {

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response) throws IOException {
    CharSequence pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    String line = pathInfo.subSequence(1, pathInfo.length()).toString();
    doClassificationDistribution(line, response);
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response) throws IOException {
    String line = request.getReader().readLine();
    if (line == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No body");
      return;
    }
    doClassificationDistribution(line, response);
  }

  private void doClassificationDistribution(String line,
                                            HttpServletResponse response) throws IOException {

    Generation generation = getGenerationManager().getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    InboundSettings inboundSettings = getInboundSettings();
    Integer targetColumn = inboundSettings.getTargetColumn();
    boolean isClassification =
        inboundSettings.getCategoricalColumns().contains(targetColumn);
    if (!isClassification) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Only supported for classification");
      return;
    }

    TreeBasedClassifier forest = generation.getForest();

    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping =
        generation.getColumnToCategoryNameToIDMapping();
    Map<Integer,String> targetIDToCategory =
        columnToCategoryNameToIDMapping.get(targetColumn).inverse();

    int totalColumns = getTotalColumns();

    String[] tokens = DelimitedDataUtils.decode(line);
    if (tokens.length != totalColumns) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
      return;
    }

    Feature[] features = new Feature[totalColumns]; // Too big by 1 but makes math easier

    try {
      for (int col = 0; col < features.length; col++) {
        if (col == inboundSettings.getTargetColumn()) {
          features[col] = IgnoredFeature.INSTANCE;
        } else {
          features[col] = buildFeature(col, tokens[col], columnToCategoryNameToIDMapping);
        }
      }
    } catch (IllegalArgumentException iae) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad input line");
      return;
    }

    Example example = new Example(null, features);
    Prediction prediction = forest.classify(example);

    Preconditions.checkState(prediction.getFeatureType() == FeatureType.CATEGORICAL);
    CategoricalPrediction categoricalPrediction = (CategoricalPrediction) prediction;
    float[] probabilities = categoricalPrediction.getCategoryProbabilities();
    Writer out = response.getWriter();
    for (int categoryID = 0; categoryID < probabilities.length; categoryID++) {
      out.write(targetIDToCategory.get(categoryID));
      out.write(',');
      out.write(Float.toString(probabilities[categoryID]));
      out.write('\n');
    }
  }

}
