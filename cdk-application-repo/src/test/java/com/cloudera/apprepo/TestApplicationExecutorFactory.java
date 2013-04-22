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

import com.cloudera.apprepo.executor.JavaMR1Executor;
import com.cloudera.apprepo.filesystem.FileSystemApplicationRepository;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class TestApplicationExecutorFactory {

  private static final Logger logger = LoggerFactory.getLogger(TestApplicationExecutorFactory.class);

  private ApplicationRepository repository;
  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
    repository = new FileSystemApplicationRepository.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .get();

    repository.deploy("test", new File(Resources.getResource("bundles/b2").getFile()));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testBuilder() {
    ApplicationExecutorFactory factory = new ApplicationExecutorFactory.Builder()
      .repository(repository)
      .registerExecutor("classpath-dependency", new DoNothingExecutor())
      .registerExecutor("java-mr1", new DoNothingExecutor())
      .get();

    logger.debug("built factory:{}", factory);

    Exception caughtEx = null;

    try {
      factory.getExecutor("non-existent-app");
    } catch (Exception e) {
      caughtEx = e;
    }

    Assert.assertNotNull(
      "getExecutor() didn't produce an exception for an unknown type", caughtEx);

    ApplicationExecutor executor = factory.getExecutor("test");
    Assert.assertNotNull(
      "getExecutor() didn't produce an executor for type test", executor);

    executor.execute();
  }

  private static class DoNothingExecutor implements ApplicationExecutorDelegate {

    private static final Logger logger = LoggerFactory.getLogger(DoNothingExecutor.class);

    @Override
    public void execute(BundleDescriptor descriptor) {
      logger.debug("Doing nothing - descriptor:{}", descriptor);
    }

  }

}
