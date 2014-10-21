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

package com.cloudera.oryx.computation;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Callable;
import javax.net.ssl.SSLContext;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServletResponse;

import com.cloudera.oryx.common.servcomp.web.InMemoryRealm;
import com.cloudera.oryx.common.servcomp.web.style_jspx;
import com.cloudera.oryx.common.settings.APISettings;
import com.cloudera.oryx.common.settings.ConfigUtils;

import com.google.common.io.Files;
import com.typesafe.config.Config;
import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.Lifecycle;
import org.apache.catalina.LifecycleEvent;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Server;
import org.apache.catalina.authenticator.DigestAuthenticator;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.JasperListener;
import org.apache.catalina.core.JreMemoryLeakPreventionListener;
import org.apache.catalina.core.ThreadLocalLeakPreventionListener;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.deploy.LoginConfig;
import org.apache.catalina.deploy.SecurityCollection;
import org.apache.catalina.deploy.SecurityConstraint;
import org.apache.catalina.startup.Tomcat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.oryx.common.servcomp.web.LogServlet;
import com.cloudera.oryx.computation.web.ComputationInitListener;
import com.cloudera.oryx.computation.web.error_jspx;
import com.cloudera.oryx.computation.web.index_jspx;
import com.cloudera.oryx.computation.web.PeriodicRunnerServlet;
import com.cloudera.oryx.common.io.IOUtils;
import com.cloudera.oryx.common.log.MemoryHandler;
import com.cloudera.oryx.common.signal.SignalManager;
import com.cloudera.oryx.common.signal.SignalType;
import com.cloudera.oryx.computation.web.status_jspx;

/**
 * <p>This class will periodically run one generation of the Computation Layer using
 * {@link com.cloudera.oryx.computation.common.DistributedGenerationRunner}. It can run after a period of time has elapsed, or an amount of
 * data has been written.</p>
 *
 * <p>Example:</p>
 *
 * <p>{@code java -Dconfig.file=[config.conf] -jar oryx-computation-x.y.jar}</p>
 *
 * @author Sean Owen
 */
public final class Runner implements Callable<Object>, Closeable {

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  private static final int[] ERROR_PAGE_STATUSES = {
      HttpServletResponse.SC_BAD_REQUEST,
      HttpServletResponse.SC_UNAUTHORIZED,
      HttpServletResponse.SC_NOT_FOUND,
      HttpServletResponse.SC_METHOD_NOT_ALLOWED,
      HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
      HttpServletResponse.SC_SERVICE_UNAVAILABLE,
  };

  private final Config config;
  private Tomcat tomcat;
  private File noSuchBaseDir;
  private boolean closed;

  public Runner() {
    this.config = ConfigUtils.getDefaultConfig();
  }

  /**
   * Starts the main loop, which runs indefinitely.
   */
  @Override
  public synchronized Object call() throws IOException {

    if (closed) {
      return Boolean.FALSE;
    }

    MemoryHandler.setSensibleLogFormat();
    java.util.logging.Logger.getLogger("").addHandler(new MemoryHandler());

    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");

    this.noSuchBaseDir = Files.createTempDir();
    this.noSuchBaseDir.deleteOnExit();

    Tomcat tomcat = new Tomcat();
    Connector connector = makeConnector();
    configureTomcat(tomcat, connector);
    configureEngine(tomcat.getEngine());
    configureServer(tomcat.getServer());
    configureHost(tomcat.getHost());
    Context context = makeContext(tomcat, noSuchBaseDir);

    context.addApplicationListener(ComputationInitListener.class.getName());

    addServlet(context, new PeriodicRunnerServlet(), "/periodicRunner/*");
    addServlet(context, new style_jspx(), "/style.jspx");
    addServlet(context, new index_jspx(), "/index.jspx");
    addServlet(context, new status_jspx(), "/status.jspx");
    addServlet(context, new error_jspx(), "/error.jspx");
    addServlet(context, new LogServlet(), "/log.txt");

    try {
      tomcat.start();
    } catch (LifecycleException le) {
      throw new IOException(le);
    }
    this.tomcat = tomcat;

    return null;
  }

  /**
   * Blocks and waits until the server shuts down.
   */
  void await() {
    Server server = null;
    synchronized (this) {
      if (tomcat != null) {
        server = tomcat.getServer();
      }
    }
    if (server != null) {
      server.await();
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      if (tomcat != null) {
        try {
          tomcat.stop();
          tomcat.destroy();
        } catch (LifecycleException le) {
          log.warn("Unexpected error while stopping", le);
        }
        try {
          IOUtils.deleteRecursively(noSuchBaseDir);
        } catch (IOException e) {
          log.warn("Failed to delete {}", noSuchBaseDir);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    final Runner runner = new Runner();
    SignalManager.register(new Runnable() {
      @Override
      public void run() {
        runner.close();
      }
    }, SignalType.INT, SignalType.TERM);
    try {
      runner.call();
      runner.await();
    } finally {
      runner.close();
    }
  }

  private void configureTomcat(Tomcat tomcat, Connector connector) {
    tomcat.setBaseDir(noSuchBaseDir.getAbsolutePath());
    tomcat.setConnector(connector);
    tomcat.getService().addConnector(connector);
  }

  private void configureEngine(Engine engine) {
    APISettings apiSettings = APISettings.create(config.getConfig("computation-layer.api"));
    String userName = apiSettings.getUserName();
    String password = apiSettings.getPassword();
    if (userName != null && password != null) {
      InMemoryRealm realm = new InMemoryRealm();
      realm.addUser(userName, password);
      engine.setRealm(realm);
    }
  }

  private static void configureServer(Server server) {
    LifecycleListener jasperListener = new JasperListener();
    server.addLifecycleListener(jasperListener);
    jasperListener.lifecycleEvent(new LifecycleEvent(server, Lifecycle.BEFORE_INIT_EVENT, null));
    server.addLifecycleListener(new JreMemoryLeakPreventionListener());
    server.addLifecycleListener(new ThreadLocalLeakPreventionListener());
  }

  private static void configureHost(Host host) {
    host.setAutoDeploy(false);
  }

  private Connector makeConnector() {
    Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
    APISettings apiSettings = APISettings.create(config.getConfig("computation-layer.api"));
    File keystoreFile = apiSettings.getKeystoreFile();
    if (keystoreFile == null) {
      // HTTP connector
      connector.setPort(apiSettings.getPort());
      connector.setSecure(false);
      connector.setScheme("http");

    } else {
      // HTTPS connector
      connector.setPort(apiSettings.getSecurePort());
      connector.setSecure(true);
      connector.setScheme("https");
      connector.setAttribute("SSLEnabled", "true");
      String protocol = chooseSSLProtocol("TLSv1.2", "TLSv1.1", "TLSv1");
      if (protocol != null) {
        connector.setAttribute("sslProtocol", protocol);
      }
      connector.setAttribute("keystoreFile", keystoreFile.getAbsoluteFile());
      connector.setAttribute("keystorePass", apiSettings.getKeystorePassword());
    }

    // Keep quiet about the server type
    connector.setXpoweredBy(false);
    connector.setAttribute("server", "Oryx");

    return connector;
  }
  
  private static String chooseSSLProtocol(String... protocols) {
    for (String protocol : protocols) {
      try {
        SSLContext.getInstance(protocol);
        return protocol;
      } catch (NoSuchAlgorithmException ignored) {
        // continue
      }
    }
    return null;
  }

  private Context makeContext(Tomcat tomcat, File noSuchBaseDir) throws IOException {

    File contextPath = new File(noSuchBaseDir, "context");
    IOUtils.mkdirs(contextPath);

    Context context = tomcat.addContext("", contextPath.getAbsolutePath());
    context.setWebappVersion("3.0");
    context.addWelcomeFile("index.jspx");
    addErrorPages(context);

    APISettings apiSettings = APISettings.create(config.getConfig("computation-layer.api"));

    boolean needHTTPS = apiSettings.isSecure();
    boolean needAuthentication = apiSettings.getUserName() != null;

    if (needHTTPS || needAuthentication) {

      SecurityCollection securityCollection = new SecurityCollection();
      securityCollection.addPattern("/*");
      SecurityConstraint securityConstraint = new SecurityConstraint();
      securityConstraint.addCollection(securityCollection);

      if (needHTTPS) {
        securityConstraint.setUserConstraint("CONFIDENTIAL");
      }

      if (needAuthentication) {

        LoginConfig loginConfig = new LoginConfig();
        loginConfig.setAuthMethod("DIGEST");
        loginConfig.setRealmName(InMemoryRealm.NAME);
        context.setLoginConfig(loginConfig);

        securityConstraint.addAuthRole(InMemoryRealm.AUTH_ROLE);

        context.addSecurityRole(InMemoryRealm.AUTH_ROLE);
        context.getPipeline().addValve(new DigestAuthenticator());
      }

      context.addConstraint(securityConstraint);
    }

    context.setCookies(false);

    return context;
  }

  private static void addServlet(Context context, Servlet servlet, String path) {
    String name = servlet.getClass().getSimpleName();
    Tomcat.addServlet(context, name, servlet).setLoadOnStartup(1);
    context.addServletMapping(path, name);
  }

  private static void addErrorPages(Context context) {
    for (int errorCode : ERROR_PAGE_STATUSES) {
      ErrorPage errorPage = new ErrorPage();
      errorPage.setErrorCode(errorCode);
      errorPage.setLocation("/error.jspx");
      context.addErrorPage(errorPage);
    }
    ErrorPage errorPage = new ErrorPage();
    errorPage.setExceptionType(Throwable.class.getName());
    errorPage.setLocation("/error.jspx");
    context.addErrorPage(errorPage);
  }

}
