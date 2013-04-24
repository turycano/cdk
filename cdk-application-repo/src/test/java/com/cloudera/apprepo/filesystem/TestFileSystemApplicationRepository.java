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
package com.cloudera.apprepo.filesystem;

import com.cloudera.apprepo.ApplicationRepository;
import com.cloudera.apprepo.BundleDescriptor;
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

public class TestFileSystemApplicationRepository {

  private static final Logger logger = LoggerFactory.getLogger(TestFileSystemApplicationRepository.class);

  private FileSystem fileSystem;
  private Path testDirectory;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
    testDirectory = new Path(Files.createTempDir().getAbsolutePath());
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @Test
  public void testDeploy() throws IOException {
    ApplicationRepository repo = new FileSystemApplicationRepository.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .get();

    logger.debug("repo:{}", repo);

    BundleDescriptor descriptor = repo.deploy("test1",
      new File(Resources.getResource("bundles/b1").getFile()));

    Assert.assertTrue("test1 directory doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test1")));
    Assert.assertTrue("test1/test.txt doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test1/test.txt")));
    Assert.assertNotNull("Descriptor is null", descriptor);
    Assert.assertEquals("1", descriptor.getVersion());
    Assert.assertEquals("classpath-dependency", descriptor.getType());

    descriptor = repo.deploy("test2", new File(Resources.getResource("bundles/b2").getFile()));

    Assert.assertTrue("test2 directory doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test2")));
    Assert.assertTrue("test2/libs directory doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test2/libs")));
    Assert.assertNotNull("Descriptor is null", descriptor);
    Assert.assertEquals("1", descriptor.getVersion());
    Assert.assertEquals("java-mr1", descriptor.getType());

    repo.deploy("test3", new File(Resources.getResource("bundles/b3.jar").getFile()));
    Assert.assertTrue("test3 directory doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test3")));
    Assert.assertTrue("test3/test3.jar file doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test3/test3.jar")));
  }

  @Test
  public void testFailedDeploy() throws IOException {
    ApplicationRepository repo = new FileSystemApplicationRepository.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .get();

    Exception caughtEx = null;

    try {
      BundleDescriptor descriptor = repo.deploy("should-fail",
        new File("i-do-not-exist"));
    } catch (IllegalArgumentException e) {
      caughtEx = e;
    }

    Assert.assertNotNull("Expected exception not thrown", caughtEx);
    Assert.assertFalse("Directory should-fail.tmp should not exist",
      fileSystem.exists(new Path(testDirectory, "should-fail.tmp")));
    Assert.assertFalse("Directory should-fail should not exist",
      fileSystem.exists(new Path(testDirectory, "should-fail")));
  }

  @Test
  public void testUndeploy() throws IOException {
    ApplicationRepository repo = new FileSystemApplicationRepository.Builder()
      .fileSystem(fileSystem)
      .directory(testDirectory)
      .get();

    repo.deploy("test1", new File(Resources.getResource("bundles/b1").getFile()));
    Assert.assertTrue("test1 directory doesn't exist",
      fileSystem.exists(new Path(testDirectory, "test1")));

    repo.undeploy("test1");
    Assert.assertFalse("test1 directory exists",
      fileSystem.exists(new Path(testDirectory, "test1")));
  }

}
