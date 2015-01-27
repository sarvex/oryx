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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.cloudera.oryx.common.io.DelimitedDataUtils;
import com.cloudera.oryx.kmeans.serving.generation.Generation;
import org.apache.commons.math3.linear.RealVector;

/**
 * <p>Responds to a GET request to {@code /assign/[datum]}, or a POST to {@code /assign}
 * containing the datum on one line. The input is one data point to cluster,
 * delimited, like "1,-4,3.0". The response body contains the ID of the nearest cluster, on one line.</p>
 *
 * @author Sean Owen
 */
public final class AssignServlet extends AbstractKMeansServlet {

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response) throws IOException {
    String pathInfo = request.getPathInfo();
    if (pathInfo == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No path");
      return;
    }
    doAssign(pathInfo.substring(1), response);
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response) throws IOException {
    String line = request.getReader().readLine();
    if (line == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No body");
      return;
    }
    doAssign(line, response);
  }

  private void doAssign(String line, HttpServletResponse response) throws IOException {

    Generation generation = getGenerationManager().getCurrentGeneration();
    if (generation == null) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                         "API method unavailable until model has been built and loaded");
      return;
    }

    RealVector vec = generation.toVector(DelimitedDataUtils.decode(line));
    if (vec == null) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Wrong column count");
      return;
    }

    int assignment = DistanceToNearestServlet.findClosest(generation, vec).getClosestCenterId();
    response.getWriter().write(Integer.toString(assignment));
  }

}
