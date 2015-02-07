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

package com.cloudera.oryx.rdf.common.pmml;

import com.cloudera.oryx.common.pmml.PMMLUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import org.apache.commons.math3.util.Pair;
import org.dmg.pmml.Array;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MissingValueStrategyType;
import org.dmg.pmml.Model;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.Node;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Predicate;
import org.dmg.pmml.ScoreDistribution;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.SimplePredicate;
import org.dmg.pmml.SimpleSetPredicate;
import org.dmg.pmml.TreeModel;
import org.dmg.pmml.True;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.cloudera.oryx.common.collection.BitSet;
import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.settings.ConfigUtils;
import com.cloudera.oryx.common.settings.InboundSettings;
import com.cloudera.oryx.rdf.common.example.FeatureType;
import com.cloudera.oryx.rdf.common.rule.CategoricalDecision;
import com.cloudera.oryx.rdf.common.rule.CategoricalPrediction;
import com.cloudera.oryx.rdf.common.rule.Decision;
import com.cloudera.oryx.rdf.common.rule.NumericDecision;
import com.cloudera.oryx.rdf.common.rule.NumericPrediction;
import com.cloudera.oryx.rdf.common.rule.Prediction;
import com.cloudera.oryx.rdf.common.tree.DecisionForest;
import com.cloudera.oryx.rdf.common.tree.DecisionNode;
import com.cloudera.oryx.rdf.common.tree.DecisionTree;
import com.cloudera.oryx.rdf.common.tree.TerminalNode;
import com.cloudera.oryx.rdf.common.tree.TreeNode;

/**
 * Contains utility methods for writing a {@link DecisionTree} as a PMML file, and reading it back.
 *
 * @author Sean Owen
 */
public final class DecisionForestPMML {

  private DecisionForestPMML() {
  }


  // Write PMML

  /**
   * Writes to a {@link File} instead of {@link Writer}.
   *
   * @see #write(Writer, DecisionForest, Map)
   */
  public static void write(File pmmlFile,
                           DecisionForest forest,
                           Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) throws IOException {
    Writer pmmlOut = IOUtils.buildGZIPWriter(pmmlFile);
    try {
      write(pmmlOut, forest, columnToCategoryNameToIDMapping);
    } finally {
      pmmlOut.close();
    }
  }

  /**
   * @param pmmlOut stream to write PMML representation to
   * @param forest {@link DecisionForest} to encode as PMML
   * @param columnToCategoryNameToIDMapping {@link Map} from column number in the input, to a {@link BiMap}
   *  mapping between category value names and category value IDs (for categorical feature columns only). This
   *  is necessary because the {@link DecisionForest} operates in terms of value IDs, but the PMML encoding
   *  should encode the names of these category values -- {@code female}, not {@code 2}
   */
  public static void write(Writer pmmlOut,
                           DecisionForest forest,
                           Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) throws IOException {

    InboundSettings inboundSettings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    int targetColumn = inboundSettings.getTargetColumn();
    boolean classificationTask = inboundSettings.isCategorical(targetColumn);

    Model model;
    if (forest.getTrees().length == 1) {
      model = buildTreeModel(columnToCategoryNameToIDMapping,
                             forest.getTrees()[0],
                             inboundSettings);
    } else {
      MiningModel miningModel = new MiningModel();
      model = miningModel;
      MultipleModelMethodType multipleModelMethodType = classificationTask ?
          MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE :
          MultipleModelMethodType.WEIGHTED_AVERAGE;

      List<Segment> segments = new ArrayList<Segment>();
      int treeID = 0;
      for (DecisionTree tree : forest) {
        TreeModel treeModel = buildTreeModel(columnToCategoryNameToIDMapping,
                                             tree,
                                             inboundSettings);
        Segment segment = new Segment();
        segment.setId(Integer.toString(treeID));
        segment.setPredicate(new True());
        segment.setModel(treeModel);
        segment.setWeight(forest.getWeights()[treeID]);
        segments.add(segment);
        treeID++;
      }

      miningModel.setSegmentation(new Segmentation(multipleModelMethodType, segments));
    }

    model.setFunctionName(classificationTask ? MiningFunctionType.CLASSIFICATION : MiningFunctionType.REGRESSION);
    model.setMiningSchema(PMMLUtils.buildMiningSchema(inboundSettings, forest.getFeatureImportances()));

    DataDictionary dictionary = PMMLUtils.buildDataDictionary(inboundSettings, columnToCategoryNameToIDMapping);
    PMML pmml = new PMML("4.2.1", null, dictionary);
    pmml.getModels().add(model);

    try {
      JAXBUtil.marshalPMML(pmml, new StreamResult(pmmlOut));
    } catch (JAXBException jaxbe) {
      throw new IOException(jaxbe);
    }
  }

  private static TreeModel buildTreeModel(Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping,
                                          DecisionTree tree,
                                          InboundSettings settings) {

    List<String> columnNames = settings.getColumnNames();
    int targetColumn = settings.getTargetColumn();

    Node root = new Node();
    root.setId("r");

    // Queue<Node> modelNodes = Queues.newArrayDeque();
    Queue<Node> modelNodes = new ArrayDeque<Node>();
    modelNodes.add(root);

    Queue<Pair<TreeNode, Decision>> treeNodes = new ArrayDeque<Pair<TreeNode, Decision>>();
    treeNodes.add(new Pair<TreeNode,Decision>(tree.getRoot(), null));

    while (!treeNodes.isEmpty()) {

      Pair<TreeNode,Decision> treeNodePredicate = treeNodes.remove();
      Node modelNode = modelNodes.remove();

      // This is the decision that got us here from the parent, if any; not the predicate at this node
      Predicate predicate = buildPredicate(treeNodePredicate.getSecond(),
                                           columnNames,
                                           columnToCategoryNameToIDMapping);
      modelNode.setPredicate(predicate);

      TreeNode treeNode = treeNodePredicate.getFirst();
      if (treeNode.isTerminal()) {

        TerminalNode terminalNode = (TerminalNode) treeNode;
        modelNode.setRecordCount((double) terminalNode.getCount());

        Prediction prediction = terminalNode.getPrediction();

        if (prediction.getFeatureType() == FeatureType.CATEGORICAL) {

          Map<Integer,String> categoryIDToName =
              columnToCategoryNameToIDMapping.get(targetColumn).inverse();
          CategoricalPrediction categoricalPrediction = (CategoricalPrediction) prediction;
          int[] categoryCounts = categoricalPrediction.getCategoryCounts();
          float[] categoryProbabilities = categoricalPrediction.getCategoryProbabilities();
          for (int categoryID = 0; categoryID < categoryProbabilities.length; categoryID++) {
            int categoryCount = categoryCounts[categoryID];
            float probability = categoryProbabilities[categoryID];
            if (categoryCount > 0 && probability > 0.0f) {
              String categoryName = categoryIDToName.get(categoryID);
              ScoreDistribution distribution = new ScoreDistribution(categoryName, categoryCount);
              distribution.setProbability((double) probability);
              modelNode.getScoreDistributions().add(distribution);
            }
          }

        } else {

          NumericPrediction numericPrediction = (NumericPrediction) prediction;
          modelNode.setScore(Float.toString(numericPrediction.getPrediction()));
        }

      } else {

        DecisionNode decisionNode = (DecisionNode) treeNode;
        Decision decision = decisionNode.getDecision();

        Node positiveModelNode = new Node();
        positiveModelNode.setId(modelNode.getId() + '+');
        modelNode.getNodes().add(positiveModelNode);
        Node negativeModelNode = new Node();
        negativeModelNode.setId(modelNode.getId() + '-');
        modelNode.getNodes().add(negativeModelNode);
        modelNode.setDefaultChild(
            decision.getDefaultDecision() ? positiveModelNode.getId() : negativeModelNode.getId());
        modelNodes.add(positiveModelNode);
        modelNodes.add(negativeModelNode);
        treeNodes.add(new Pair<TreeNode,Decision>(decisionNode.getRight(), decision));
        treeNodes.add(new Pair<TreeNode,Decision>(decisionNode.getLeft(), null));

      }

    }

    TreeModel treeModel = new TreeModel();
    treeModel.setNode(root);
    treeModel.setSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT);
    treeModel.setMissingValueStrategy(MissingValueStrategyType.DEFAULT_CHILD);
    return treeModel;
  }

  private static Predicate buildPredicate(Decision decision,
                                          List<String> columnNames,
                                          Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping) {
    Predicate predicate;
    if (decision == null) {
      predicate = new True();

    } else {
      int columnNumber = decision.getFeatureNumber();
      FieldName fieldName = new FieldName(columnNames.get(columnNumber));

      if (decision.getType() == FeatureType.CATEGORICAL) {
        CategoricalDecision categoricalDecision = (CategoricalDecision) decision;
        Map<Integer,String> categoryIDToName = columnToCategoryNameToIDMapping.get(columnNumber).inverse();
        BitSet includedCategoryIDs = categoricalDecision.getCategoryIDs();
        List<String> categoryNames = Lists.newArrayList();
        int categoryID = -1;
        while ((categoryID = includedCategoryIDs.nextSetBit(categoryID + 1)) >= 0) {
          categoryNames.add(categoryIDToName.get(categoryID));
        }
        Array categories = new Array(Array.Type.STRING, DelimitedDataUtils.encode(' ', categoryNames));
        predicate = new SimpleSetPredicate(fieldName, SimpleSetPredicate.BooleanOperator.IS_IN, categories);

      } else {
        NumericDecision numericDecision = (NumericDecision) decision;
        SimplePredicate numericPredicate = new SimplePredicate(fieldName, SimplePredicate.Operator.GREATER_OR_EQUAL);
        numericPredicate.setValue(Float.toString(numericDecision.getThreshold()));
        predicate = numericPredicate;
      }
    }
    return predicate;
  }


  // Read PMML

  /**
   * @param pmmlFile file to read PMML encoding from
   * @return a {@link DecisionForest} representation of the PMML encoded model
   */
  public static Pair<DecisionForest, Map<Integer,BiMap<String,Integer>>> read(File pmmlFile) throws IOException {

    PMML pmml;
    InputStream in = IOUtils.openMaybeDecompressing(pmmlFile);
    try {
      pmml = JAXBUtil.unmarshalPMML(ImportFilter.apply(new InputSource(in)));
    } catch (JAXBException e) {
      throw new IOException(e);
    } catch (SAXException e) {
      throw new IOException(e);
    } finally {
      in.close();
    }

    InboundSettings settings = InboundSettings.create(ConfigUtils.getDefaultConfig());
    Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping =
        PMMLUtils.buildColumnCategoryMapping(pmml.getDataDictionary());

    DecisionTree[] trees;
    double[] weights;

    List<Model> models = pmml.getModels();
    Model model = models.get(0);
    if (model instanceof MiningModel) {
      MiningModel miningModel = (MiningModel) model;
      List<Segment> segments = miningModel.getSegmentation().getSegments();
      Preconditions.checkArgument(!segments.isEmpty());

      trees = new DecisionTree[segments.size()];
      weights = new double[trees.length];
      for (int i = 0; i < trees.length; i++) {
        Segment segment = segments.get(i);
        weights[i] = segment.getWeight();
        TreeModel treeModel = (TreeModel) segment.getModel();
        TreeNode root = translateFromPMML(treeModel.getNode(), columnToCategoryNameToIDMapping, settings);
        trees[i] = new DecisionTree(root);
      }
    } else {
      // Single tree model
      TreeNode root = translateFromPMML(((TreeModel) model).getNode(), columnToCategoryNameToIDMapping, settings);
      trees = new DecisionTree[] { new DecisionTree(root) };
      weights = new double[] { 1.0 };
    }

    List<String> columnNames = settings.getColumnNames();
    double[] featureImportances = new double[columnNames.size()];
    for (MiningField field : model.getMiningSchema().getMiningFields()) {
      Double importance = field.getImportance();
      if (importance != null) {
        int featureNumber = columnNames.indexOf(field.getName().getValue());
        featureImportances[featureNumber] = importance;
      }
    }

    return new Pair<DecisionForest, Map<Integer, BiMap<String, Integer>>>(
        new DecisionForest(trees, weights, featureImportances),
        columnToCategoryNameToIDMapping);
  }

  private static TreeNode translateFromPMML(Node root,
                                            Map<Integer,BiMap<String,Integer>> columnToCategoryNameToIDMapping,
                                            InboundSettings settings) {

    List<String> columnNames = settings.getColumnNames();
    int targetColumn = settings.getTargetColumn();

    List<Node> children = root.getNodes();
    if (children.isEmpty()) {
      // Terminal
      Collection<ScoreDistribution> scoreDistributions = root.getScoreDistributions();
      Prediction prediction;
      if (scoreDistributions != null && !scoreDistributions.isEmpty()) {
        // Categorical target
        Map<String,Integer> valueToID = columnToCategoryNameToIDMapping.get(targetColumn);
        int[] categoryCounts = new int[valueToID.size()];
        for (ScoreDistribution dist : scoreDistributions) {
          int valueID = valueToID.get(dist.getValue());
          categoryCounts[valueID] = (int) Math.round(dist.getRecordCount());
        }
        prediction = new CategoricalPrediction(categoryCounts);
      } else {
        prediction = new NumericPrediction(Float.parseFloat(root.getScore()), (int) Math.round(root.getRecordCount()));
      }
      return new TerminalNode(prediction);
    }

    Preconditions.checkArgument(children.size() == 2);
    // Decision
    Node child1 = children.get(0);
    Node child2 = children.get(1);
    Node negativeLeftChild;
    Node positiveRightChild;
    if (child1.getPredicate().getClass().equals(True.class)) {
      negativeLeftChild = child1;
      positiveRightChild = child2;
    } else {
      Preconditions.checkArgument(child2.getPredicate().getClass().equals(True.class));
      negativeLeftChild = child2;
      positiveRightChild = child1;
    }

    Decision decision;
    Predicate predicate = positiveRightChild.getPredicate();
    boolean defaultDecision = positiveRightChild.getId().equals(root.getDefaultChild());

    if (predicate instanceof SimplePredicate) {
      // Numeric decision
      SimplePredicate simplePredicate = (SimplePredicate) predicate;
      Preconditions.checkArgument(simplePredicate.getOperator() == SimplePredicate.Operator.GREATER_OR_EQUAL);
      float threshold = Float.parseFloat(simplePredicate.getValue());
      int featureNumber = columnNames.indexOf(simplePredicate.getField().getValue());
      decision = new NumericDecision(featureNumber, threshold, defaultDecision);

    } else {
      // Categorical decision
      Preconditions.checkArgument(predicate instanceof SimpleSetPredicate);
      SimpleSetPredicate simpleSetPredicate = (SimpleSetPredicate) predicate;
      Preconditions.checkArgument(simpleSetPredicate.getBooleanOperator() == SimpleSetPredicate.BooleanOperator.IS_IN);
      int featureNumber = columnNames.indexOf(simpleSetPredicate.getField().getValue());
      Map<String,Integer> categoryNameToID = columnToCategoryNameToIDMapping.get(featureNumber);
      String[] categories = DelimitedDataUtils.decode(simpleSetPredicate.getArray().getValue(), ' ');
      BitSet activeCategories = new BitSet(categoryNameToID.size());
      for (String category : categories) {
        int categoryID = categoryNameToID.get(category);
        activeCategories.set(categoryID);
      }
      decision = new CategoricalDecision(featureNumber, activeCategories, defaultDecision);
    }

    return new DecisionNode(decision,
                            translateFromPMML(negativeLeftChild, columnToCategoryNameToIDMapping, settings),
                            translateFromPMML(positiveRightChild, columnToCategoryNameToIDMapping, settings));
  }

}
