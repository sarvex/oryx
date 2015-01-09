package com.cloudera.oryx.als.serving.web;

import com.cloudera.oryx.als.common.NotReadyException;
import com.cloudera.oryx.als.common.OryxRecommender;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * <p>Responds to a GET request to {@code /user/allIDs}
 * and in turn calls {@link com.cloudera.oryx.als.common.OryxRecommender#getAllUserIDs()}.</p>
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
