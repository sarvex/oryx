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

package com.cloudera.oryx.kmeans.serving.web;

import com.google.common.io.CharStreams;
import org.apache.commons.math3.linear.RealVector;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.kmeans.serving.generation.Generation;
import com.cloudera.oryx.kmeans.serving.generation.KMeansGenerationManager;

/**
 * <p>Responds to POST request to {@code /add}. The input is one or more data points
 * to add to the clustering, one for each line of the request body. Each data point is a delimited line of input like
 * "1,-4,3.0". Also, one data point can be supplied by POSTing to {@code /train/[datum]}.
 * The clusters update to learn in some way from the new data. The response is empty.</p>
 *
 * @author Sean Owen
 */
public final class AddServlet extends AbstractKMeansServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String pathInfo = request.getPathInfo();
    if (pathInfo == null || pathInfo.isEmpty() || "/".equals(pathInfo)) {
      doAdd(CharStreams.readLines(request.getReader()), response);
    } else {
      doAdd(Collections.singletonList(pathInfo.substring(1)), response);
    }
  }

  private void doAdd(List<String> lines, HttpServletResponse response) throws IOException {

    KMeansGenerationManager generationManager = getGenerationManager();
    Generation generation = generationManager.getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    for (CharSequence line : lines) {
      RealVector vec = generation.toVector(DelimitedDataUtils.decode(line));
      if (vec == null) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
        return;
      }
      generationManager.append(line);

      // TODO update the centers, along the lines of Meyerson et al.
    }

  }

}
