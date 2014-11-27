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

package com.cloudera.oryx.common.servcomp;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import com.cloudera.oryx.common.settings.ConfigUtils;

/**
 * Represents resource names on HDFS.
 * 
 * @author Sean Owen
 */
public final class Namespaces {

  private static final Logger log = LoggerFactory.getLogger(Namespaces.class);

  private static final int DIGITS_IN_GENERATION_ID = 5;
  public static final int MAX_GENERATION = Integer.parseInt(Strings.repeat("9", DIGITS_IN_GENERATION_ID));

  private static final Namespaces instance = new Namespaces();

  private final String prefix;

  private Namespaces() {
    Config config = ConfigUtils.getDefaultConfig();
    boolean localData;
    if (config.hasPath("model.local")) {
      log.warn("model.local is deprecated; use model.local-data");
      localData = config.getBoolean("model.local");
    } else {
      localData = config.getBoolean("model.local-data");
    }
    if (localData) {
      prefix = "file:";
    } else {
      URI defaultURI = FileSystem.getDefaultUri(OryxConfiguration.get());
      String host = defaultURI.getHost();
      Preconditions.checkNotNull(host,
          "Hadoop FS has no host in '%s'. Did you intend to set model.local-data=true?",
          defaultURI);
      int port = defaultURI.getPort();
      if (port > 0) {
        prefix = "hdfs://" + host + ':' + port;
      } else {
        prefix = "hdfs://" + host;
      }
    }
    log.info("Namespace prefix: {}", prefix);
  }

  /**
   * @return singleton {@code Namespaces} instance
   */
  public static Namespaces get() {
    return instance;
  }

  public String getPrefix() {
    return prefix;
  }

  /**
   * @param suffix directory name
   * @return {@link Path} appropriate for use with Hadoop representing this directory
   */
  public static Path toPath(String suffix) {
    return new Path(get().getPrefix() + suffix);
  }

  public static String getInstancePrefix(String instanceDir) {
    return instanceDir + '/';
  }

  public static String getInstanceGenerationPrefix(String instanceDir, int generationID) {
    Preconditions.checkArgument(generationID >= 0L, "Bad generation %s", generationID);
    return getInstancePrefix(instanceDir) + getPaddedGenerationID(generationID) + '/';
  }

  private static String getPaddedGenerationID(int generationID) {
    return Strings.padStart(Integer.toString(generationID), DIGITS_IN_GENERATION_ID, '0');
  }

  public static String getTempPrefix(String instanceDir, int generationID) {
    return getInstanceGenerationPrefix(instanceDir, generationID) + "tmp/";
  }

  public static String getIterationsPrefix(String instanceDir, int generationID) {
    return getTempPrefix(instanceDir, generationID) + "iterations/";
  }

  public static String getGenerationDoneKey(String instanceDir, int generationID) {
    return getInstanceGenerationPrefix(instanceDir, generationID) + "_SUCCESS";
  }

}
