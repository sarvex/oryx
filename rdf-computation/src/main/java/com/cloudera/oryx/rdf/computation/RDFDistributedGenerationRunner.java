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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.dmg.pmml.DataDictionary;
import org.dmg.pmml.Field;
import org.dmg.pmml.MiningField;
import org.dmg.pmml.MiningFunctionType;
import org.dmg.pmml.MiningModel;
import org.dmg.pmml.MiningSchema;
import org.dmg.pmml.Model;
import org.dmg.pmml.MultipleModelMethodType;
import org.dmg.pmml.PMML;
import org.dmg.pmml.Segment;
import org.dmg.pmml.Segmentation;
import org.dmg.pmml.True;
import org.dmg.pmml.TypeDefinitionField;
import org.dmg.pmml.Value;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.iterator.FileLineIterable;
import com.cloudera.oryx.common.servcomp.Namespaces;
import com.cloudera.oryx.common.servcomp.Store;
import com.cloudera.oryx.computation.common.DependsOn;
import com.cloudera.oryx.computation.common.DistributedGenerationRunner;
import com.cloudera.oryx.computation.common.JobStep;
import com.cloudera.oryx.computation.common.JobStepConfig;
import com.cloudera.oryx.rdf.computation.build.BuildTreesStep;
import com.cloudera.oryx.rdf.computation.build.MergeNewOldStep;

/**
 * @author Sean Owen
 */
public final class RDFDistributedGenerationRunner extends DistributedGenerationRunner {

  private static final Logger log = LoggerFactory.getLogger(RDFDistributedGenerationRunner.class);

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPreDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getIterationDependencies() {
    return Collections.singletonList(
        DependsOn.<Class<? extends JobStep>>nextAfterFirst(BuildTreesStep.class, MergeNewOldStep.class));
  }

  @Override
  protected List<DependsOn<Class<? extends JobStep>>> getPostDependencies() {
    return Collections.emptyList();
  }

  @Override
  protected JobStepConfig buildConfig(int iteration) {
    return new RDFJobStepConfig(getInstanceDir(),
                                getGenerationID(),
                                getLastGenerationID(),
                                iteration);
  }

  @Override
  protected void doPost() throws IOException {

    String instanceGenerationPrefix =
        Namespaces.getInstanceGenerationPrefix(getInstanceDir(), getGenerationID());
    String outputPathKey = instanceGenerationPrefix + "trees/";
    Store store = Store.get();
    PMML joinedPMML = null;

    // TODO This is still loading all trees into memory, which can be quite large.
    // To do better we would have to manage XML output more directly.

    Map<String,Mean> columnNameToMeanImportance = Maps.newHashMap();

    for (String treePrefix : store.list(outputPathKey, true)) {
      log.info("Reading trees from file {}", treePrefix);
      for (String treePMMLAsLine : new FileLineIterable(store.readFrom(treePrefix))) {
        PMML nextPMML;
        try {
          nextPMML = JAXBUtil.unmarshalPMML(
              ImportFilter.apply(new InputSource(new StringReader(treePMMLAsLine))));
        } catch (JAXBException e) {
          throw new IOException(e);
        } catch (SAXException e) {
          throw new IOException(e);
        }

        updateMeanImportances(columnNameToMeanImportance,
                              nextPMML.getModels().get(0).getMiningSchema().getMiningFields());

        if (joinedPMML == null) {

          // No prior model. Just use this one as the current model.
          joinedPMML = nextPMML;

        } else {

          Model currentModel = joinedPMML.getModels().get(0);
          Segmentation segmentation;
          if (currentModel instanceof MiningModel) {
            // Already have a MiningModel, to which new models can be added in its Segmentation
            segmentation = ((MiningModel) currentModel).getSegmentation();
          } else {
            // Lone tree model. Build a MiningModel around it, first, and swap it in
            boolean classificationTask = currentModel.getFunctionName() == MiningFunctionType.CLASSIFICATION;
            MiningModel miningModel = new MiningModel();
            MultipleModelMethodType multipleModelMethodType = classificationTask ?
                MultipleModelMethodType.WEIGHTED_MAJORITY_VOTE :
                MultipleModelMethodType.WEIGHTED_AVERAGE;
            segmentation = new Segmentation(multipleModelMethodType);
            miningModel.setSegmentation(segmentation);
            miningModel.setFunctionName(currentModel.getFunctionName());
            MiningSchema cloneSchema = new MiningSchema();
            for (MiningField field : currentModel.getMiningSchema().getMiningFields()) {
              cloneSchema.getMiningFields().add(
                  new MiningField(field.getName())
                      .withOptype(field.getOptype()
                      ).withImportance(field.getImportance()));
            }
            miningModel.setMiningSchema(cloneSchema);

            Segment segment = new Segment();
            // This will get overriden below when renumbering happens
            segment.setId("0");
            segment.setPredicate(new True());
            segment.setModel(currentModel);
            // Don't actually know the weight, so use 1
            segment.setWeight(1.0);
            segmentation.getSegments().add(segment);
            // Swap in new model
            joinedPMML.getModels().clear();
            joinedPMML.getModels().add(miningModel);
          }

          Model nextModel = nextPMML.getModels().get(0);
          if (nextModel instanceof MiningModel) {
            segmentation.getSegments().addAll(((MiningModel) nextModel).getSegmentation().getSegments());
          } else {
            Segment segment = new Segment();
            // This will get overriden below when renumbering happens
            segment.setId("0");
            segment.setPredicate(new True());
            segment.setModel(nextModel);
            // Don't actually know the weight, so use 1
            segment.setWeight(1.0);
            segmentation.getSegments().add(segment);
          }

          updateDataDictionary(joinedPMML.getDataDictionary(), nextPMML.getDataDictionary());
        }
      }
    }

    Preconditions.checkNotNull(joinedPMML, "No forests to join?");

    Model model = joinedPMML.getModels().get(0);

    if (model instanceof MiningModel) {

      // Renumber segments with distinct IDs
      List<Segment> segments = ((MiningModel) model).getSegmentation().getSegments();
      for (int treeID = 0; treeID < segments.size(); treeID++) {
        segments.get(treeID).setId(Integer.toString(treeID));
      }

      // Stitch together feature importances; only needed in an ensemble
      for (MiningField field : model.getMiningSchema().getMiningFields()) {
        String name = field.getName().getValue();
        Mean importance = columnNameToMeanImportance.get(name);
        if (importance == null) {
          field.setImportance(null);
        } else {
          field.setImportance(importance.getResult());
        }
      }
    }

    log.info("Writing combined model file");
    File tempJoinedForestFile = File.createTempFile("model-", ".pmml.gz");
    tempJoinedForestFile.deleteOnExit();
    OutputStream out = IOUtils.buildGZIPOutputStream(new FileOutputStream(tempJoinedForestFile));
    try {
      JAXBUtil.marshalPMML(joinedPMML, new StreamResult(out));
    } catch (JAXBException e) {
      throw new IOException(e);
    } finally {
      out.close();
    }

    log.info("Uploading combined model file");
    store.upload(instanceGenerationPrefix + "model.pmml.gz", tempJoinedForestFile, false);
    IOUtils.delete(tempJoinedForestFile);
  }

  private static void updateMeanImportances(Map<String,Mean> columnNameToMeanImportance,
                                            List<MiningField> miningFields) {
    for (MiningField field : miningFields) {
      Double importance = field.getImportance();
      if (importance != null) {
        String fieldName = field.getName().getValue();
        Mean mean = columnNameToMeanImportance.get(fieldName);
        if (mean == null) {
          mean = new Mean();
          columnNameToMeanImportance.put(fieldName, mean);
        }
        mean.increment(importance);
      }
    }
  }

  private static void updateDataDictionary(DataDictionary existingDict, DataDictionary newDict) {
    for (TypeDefinitionField newField : newDict.getDataFields()) {
      Collection<Value> newValues = newField.getValues();
      if (newValues != null && !newValues.isEmpty()) {
        for (TypeDefinitionField existingField : existingDict.getDataFields()) {
          if (existingField.getName().equals(newField.getName())) {
            Set<String> existingStrings = new HashSet<String>(existingField.getValues().size());
            for (Value existingValue : existingField.getValues()) {
              existingStrings.add(existingValue.getValue());
            }
            for (Value newValue : newValues) {
              if (!existingStrings.contains(newValue.getValue())) {
                existingField.getValues().add(newValue);
              }
            }
            break;
          }
        }
      }
    }
  }

}
