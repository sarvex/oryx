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

package com.cloudera.oryx.serving.web;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;

import com.cloudera.oryx.common.LangUtils;
import com.cloudera.oryx.serving.stats.ServletStats;

/**
 * Superclass of {@link HttpServlet}s used in the application. All API methods return the following
 * HTTP statuses in certain situations:
 *
 * <ul>
 *  <li>{@code 400 Bad Request} if the arguments are invalid</li>
 *  <li>{@code 401 Unauthorized} if a username/password is required, but not supplied correctly 
 *  in the request via HTTP DIGEST</li>
 *  <li>{@code 405 Method Not Allowed} if an incorrect HTTP method is used, like {@code GET} 
 *  where {@code POST} is required</li>
 *  <li>{@code 500 Internal Server Error} if an unexpected server-side exception occurs</li>
 *  <li>{@code 503 Service Unavailable} if not yet available to serve requests</li>
 * </ul>
 *
 * @author Sean Owen
 */
public abstract class AbstractOryxServlet extends HttpServlet {

  private static final String KEY_PREFIX = AbstractOryxServlet.class.getName();
  public static final String TIMINGS_KEY = KEY_PREFIX + ".TIMINGS";

  private static final Pattern ESCAPED_SLASH = Pattern.compile("%2F", Pattern.CASE_INSENSITIVE);
  protected static final Splitter SLASH = Splitter.on('/').omitEmptyStrings();
  private static final Splitter COMMA = Splitter.on(',').omitEmptyStrings().trimResults();

  private ServletStats timing;
  private ConcurrentMap<String,ResponseContentType> responseTypeCache;

  @Override
  public void init(ServletConfig config) throws ServletException {
    super.init(config);

    ServletContext context = config.getServletContext();

    Map<String,ServletStats> timings;
    synchronized (context) {
      @SuppressWarnings("unchecked")
      Map<String,ServletStats> temp = (Map<String,ServletStats>) context.getAttribute(TIMINGS_KEY);
      timings = temp;
      if (timings == null) {
        timings = Maps.newTreeMap();
        context.setAttribute(TIMINGS_KEY, timings);
      }
    }

    String key = getClass().getSimpleName();
    ServletStats theTiming = timings.get(key);
    if (theTiming == null) {
      theTiming = new ServletStats();
      timings.put(key, theTiming);
    }
    timing = theTiming;

    responseTypeCache = Maps.newConcurrentMap();
  }

  @Override
  protected final void service(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    // Default content type is just text
    response.setContentType("text/plain; charset=UTF-8");

    long start = System.nanoTime();
    super.service(request, response);
    timing.addTimingNanosec(System.nanoTime() - start);

    int status = response.getStatus();
    if (status >= 400) {
      if (status >= 500) {
        timing.incrementServerErrors();
      } else {
        timing.incrementClientErrors();
      }
    }
  }

  /**
   * Hack: we have to double-escape forward-slash so that Tomcat won't read it as a path delimiter,
   * then un-escape again on the other end.
   *
   * @param pathElement path element potentially containing %2F
   * @return argument, with %2F converted to /
   */
  protected static String unescapeSlashHack(CharSequence pathElement) {
    return ESCAPED_SLASH.matcher(pathElement).replaceAll("/");
  }

  protected static void unescapeSlashHack(String[] pathElements) {
    for (int i = 0; i < pathElements.length; i++) {
      pathElements[i] = unescapeSlashHack(pathElements[i]);
    }
  }

  protected final void output(HttpServletRequest request,
                              ServletResponse response,
                              Collection<?> values) throws IOException {
    if (values.isEmpty()) {
      return;
    }
    Writer writer = response.getWriter();
    switch (determineResponseType(request)) {
      case JSON:
        response.setContentType("application/json");
        // Single value written alone
        if (values.size() == 1) {
          writer.write(String.valueOf(values.iterator().next()));
          writer.write('\n');
        } else {
          // Many values written as array
          writer.write('[');
          boolean first = true;
          for (Object value : values) {
            if (first) {
              first = false;
            } else {
              writer.write(',');
            }
            writer.write(String.valueOf(value));
          }
          writer.write("]\n");
        }
        break;
      case DELIMITED:
        // Leave content type at default
        for (Object value : values) {
          writer.write(String.valueOf(value));
          writer.write('\n');
        }
        break;
      default:
        throw new IllegalStateException("Unknown response type");
    }
  }

  protected final void output(HttpServletRequest request,
                              ServletResponse response,
                              float[] values) throws IOException {
    // Have to translate, so go straight to Strings
    List<String> stringValues = Lists.newArrayListWithCapacity(values.length);
    for (float value : values) {
      stringValues.add(Float.toString(value));
    }
    output(request, response, stringValues);
  }

  protected final void output(HttpServletRequest request,
                              ServletResponse response,
                              double[] values) throws IOException {
    // Have to translate, so go straight to Strings
    List<String> stringValues = Lists.newArrayListWithCapacity(values.length);
    for (double value : values) {
      stringValues.add(Double.toString(value));
    }
    output(request, response, stringValues);
  }

  /**
   * Determines the appropriate content type for the response based on request headers. At the moment these
   * are chosen from the values in {@link ResponseContentType}.
   */
  protected final ResponseContentType determineResponseType(HttpServletRequest request) {

    String acceptHeader = request.getHeader(HttpHeaders.ACCEPT);
    if (acceptHeader == null) {
      return ResponseContentType.DELIMITED;
    }
    ResponseContentType cached = responseTypeCache.get(acceptHeader);
    if (cached != null) {
      return cached;
    }

    SortedMap<Double,ResponseContentType> types = Maps.newTreeMap();
    for (String accept : COMMA.split(acceptHeader)) {
      double preference;
      String type;
      int semiColon = accept.indexOf(';');
      if (semiColon < 0) {
        preference = 1.0;
        type = accept;
      } else {
        String valueString = accept.substring(semiColon + 1).trim();
        if (valueString.startsWith("q=")) {
          valueString = valueString.substring(2);
        }
        try {
          preference = LangUtils.parseDouble(valueString);
        } catch (IllegalArgumentException ignored) {
          preference = 1.0;
        }
        type = accept.substring(semiColon);
      }
      ResponseContentType parsedType = null;
      if ("text/csv".equals(type) || "text/plain".equals(type)) {
        parsedType = ResponseContentType.DELIMITED;
      } else if ("application/json".equals(type)) {
        parsedType = ResponseContentType.JSON;
      }
      if (parsedType != null) {
        types.put(preference, parsedType);
      }
    }

    ResponseContentType finalType;
    if (types.isEmpty()) {
      finalType = ResponseContentType.DELIMITED;
    } else {
      finalType = types.values().iterator().next();
    }

    responseTypeCache.putIfAbsent(acceptHeader, finalType);
    return finalType;
  }

  /**
   * Available content types / formats for response bodies.
   */
  protected enum ResponseContentType {
    JSON,
    DELIMITED,
  }

}
