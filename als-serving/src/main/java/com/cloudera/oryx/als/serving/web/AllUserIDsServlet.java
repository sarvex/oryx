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

package com.cloudera.oryx.als.serving.web;

import com.cloudera.oryx.als.common.NotReadyException;
import com.cloudera.oryx.als.common.OryxRecommender;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * <p>Responds to a GET request to {@code /user/allIDs}
 * and in turn calls {@link OryxRecommender#getAllUserIDs()}.</p>
 *
 * <p>CSV output is one user ID per line. JSON output is an array of user IDs.</p>
 *
 */
public final class AllUserIDsServlet extends AbstractALSServlet {

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    OryxRecommender recommender = getRecommender();
    try {
      output(request, response, recommender.getAllUserIDs());
    } catch (NotReadyException nre) {
      response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, nre.toString());
    }
  }
}
