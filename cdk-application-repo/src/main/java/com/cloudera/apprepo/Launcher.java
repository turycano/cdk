/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.apprepo;

import com.cloudera.apprepo.filesystem.FileSystemApplicationRepository;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.BundleException;
import org.osgi.framework.BundleListener;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.launch.Framework;
import org.osgi.framework.launch.FrameworkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.ServiceLoader;

public class Launcher implements BundleActivator, FrameworkListener, BundleListener {

  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

  private Framework framework;

  public static void main(String[] args) {
    Launcher app = new Launcher();

    app.run();
  }

  @Override
  public void start(BundleContext context) throws Exception {
    logger.info("Starting with context:{}", context);
  }

  @Override
  public void stop(BundleContext context) throws Exception {
    logger.info("Stopping with context:{}", context);
  }

  private void run() {
    ServiceLoader<FrameworkFactory> loader = ServiceLoader.load(FrameworkFactory.class);

    Map<String, String> configuration = Maps.newHashMap();

    configuration.put(Constants.FRAMEWORK_STORAGE, "framework-storage");

    for (FrameworkFactory factory : loader) {
      logger.debug("Detected OSGi FrameworkFactory:{}", factory);
      framework = factory.newFramework(configuration);
      break;
    }

    Preconditions.checkArgument(framework != null, "Unable to find an OSGi framework");

    logger.debug("Loaded framework:{}", framework);

    try {
      ApplicationRepository appRepo = new FileSystemApplicationRepository.Builder()
        .fileSystem(FileSystem.get(new Configuration()))
        .directory(new Path("test-repo"))
        .get();

      appRepo.deploy("test-app", new File("cdk-application-repo/src/test/resources/bundles/b3.jar"));
      InputSupplier<? extends InputStream> bundleSupplier = appRepo.getBundleSupplier("test-app");

      Files.copy(bundleSupplier, new File("to-deploy.jar"));

      framework.init();

      BundleContext bundleContext = framework.getBundleContext();

      bundleContext.addFrameworkListener(this);
      bundleContext.addBundleListener(this);

      logger.debug("Starting framework");
      framework.start();

      logger.debug("Installing bundle");
      Bundle bundle = bundleContext.installBundle("file:" + new File("to-deploy.jar").getAbsolutePath());

      logger.debug("Starting bundle:{}", bundle);
      bundle.start();

      logger.debug("Stopping framework");
      framework.stop();

      while (framework.getState() != Framework.RESOLVED) {
        logger.debug("Waiting for framework to stop...");
        framework.waitForStop(10000);
      }

      logger.debug("Framework stopped");

      new File("to-deploy.jar").delete();
    } catch (BundleException e) {
      logger.error("Bundle exception: " + e.getMessage(), e);
    } catch (InterruptedException e) {
      e.printStackTrace();  // TODO: Unhandled catch block!
    } catch (IOException e) {
      e.printStackTrace();  // TODO: Unhandled catch block!
    }
  }

  @Override
  public void frameworkEvent(FrameworkEvent event) {
    logger.debug("Framework event:{} type:{}", event, frameworkEventName(event.getType()));
  }

  @Override
  public void bundleChanged(BundleEvent event) {
    logger.debug("Bundle event:{} type:{}", event, bundleEventName(event.getType()));
  }

  public static String bundleEventName(int eventType) {
    switch (eventType) {
      case BundleEvent.INSTALLED:
        return "INSTALLED";
      case BundleEvent.LAZY_ACTIVATION:
        return "LAZY_ACTIVATION";
      case BundleEvent.RESOLVED:
        return "RESOLVED";
      case BundleEvent.STARTED:
        return "STARTED";
      case BundleEvent.STARTING:
        return "STARTING";
      case BundleEvent.STOPPED:
        return "STOPPED";
      case BundleEvent.STOPPING:
        return "STOPPING";
      case BundleEvent.UNINSTALLED:
        return "UNINSTALLED";
      case BundleEvent.UNRESOLVED:
        return "UNRESOLVED";
      case BundleEvent.UPDATED:
        return "UPDATED";
      default:
        return "UNKNOWN";
    }
  }

  public static String frameworkEventName(int eventType) {
    switch (eventType) {
      case FrameworkEvent.ERROR:
        return "ERROR";
      case FrameworkEvent.INFO:
        return "INFO";
      case FrameworkEvent.PACKAGES_REFRESHED:
        return "PACKAGES_REFRESHED";
      case FrameworkEvent.STARTED:
        return "STARTED";
      case FrameworkEvent.STARTLEVEL_CHANGED:
        return "STARTLEVEL_CHANGED";
      case FrameworkEvent.STOPPED:
        return "STOPPED";
      case FrameworkEvent.STOPPED_BOOTCLASSPATH_MODIFIED:
        return "STOPPED_BOOTCLASSPATH_MODIFIED";
      default:
        return "UNKNOWN";
    }
  }

  public static String frameworkStateName(int state) {
    switch (state) {
      case Framework.ACTIVE:
        return "ACTIVE";
      case Framework.INSTALLED:
        return "INSTALLED";
      case Framework.RESOLVED:
        return "RESOLVED";
      case Framework.STARTING:
        return "STARTING";
      case Framework.STOPPING:
        return "STOPPING";
      case Framework.UNINSTALLED:
        return "UNINSTALLED";
      default:
        return "OTHER";
    }
  }

}
