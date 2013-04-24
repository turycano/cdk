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
import com.cloudera.apprepo.ApplicationRepositoryException;
import com.cloudera.apprepo.BundleDescriptor;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class FileSystemApplicationRepository implements ApplicationRepository {

  private static final Logger logger = LoggerFactory.getLogger(FileSystemApplicationRepository.class);

  private FileSystem fileSystem;
  private Path directory;

  public FileSystemApplicationRepository(FileSystem fileSystem, Path directory) {
    this.fileSystem = fileSystem;
    this.directory = directory;
  }

  public void deploy(File bundle) {
    deploy(Files.getNameWithoutExtension(bundle.getName()), bundle);
  }

  @Override
  public BundleDescriptor deploy(String name, File bundle) {
    Preconditions.checkArgument(name != null, "Name can not be null");
    Preconditions.checkArgument(bundle != null, "Bundle file can not be null");
    Preconditions.checkArgument(bundle.exists(), "Bundle must exist");

    Path applicationDirectoryTmp = new Path(directory, name + ".tmp");
    Path applicationDirectory = new Path(directory, name);

    logger.info("Deploying application bundle:{} from:{} to directory:{}",
      name, bundle, applicationDirectory);

    try {
      if (!fileSystem.mkdirs(applicationDirectoryTmp)) {
        throw new ApplicationRepositoryException(
          "Failed to create application directory:" + applicationDirectoryTmp);
      }
    } catch (IOException e) {
      throw new ApplicationRepositoryException(
        "Internal error creating application directory:" + applicationDirectoryTmp, e);
    }

    BundleDescriptor descriptor;

    if (bundle.isDirectory()) {
      descriptor = deployDirectory(bundle, applicationDirectoryTmp);
    } else if (bundle.isFile()) {
      descriptor = deployFile(name, bundle, applicationDirectoryTmp);
    } else {
      throw new ApplicationRepositoryException(
        "Don't know how to deploy bundle:" + bundle + " (not a file or directory)");
    }

    try {
      if (!fileSystem.rename(applicationDirectoryTmp, applicationDirectory)) {
        throw new ApplicationRepositoryException(
          "Failed to rename application directory from:"
            + applicationDirectoryTmp + " to:" + applicationDirectory);
      }
    } catch (IOException e) {
      throw new ApplicationRepositoryException(
        "Internal error renaming application directory from:"
          + applicationDirectoryTmp + " to:" + applicationDirectory);
    }

    logger.debug("Deployed bundle with descriptor:{}", descriptor);

    return descriptor;
  }

  private BundleDescriptor deployFile(String name, File bundle, Path applicationDirectory) {
    Path bundlePath = new Path(bundle.getAbsolutePath());

    try {
      fileSystem.copyFromLocalFile(false, true, bundlePath,
        new Path(applicationDirectory, name + ".jar"));
    } catch (IOException e) {
      // TODO: Specialize this exception. -esammer
      throw Throwables.propagate(e);
    }

    return null;
  }

  private BundleDescriptor deployDirectory(File bundle, Path applicationDirectory) {
    List<Path> bundleItemPaths = Lists.newArrayList();

    BundleDescriptor descriptor = BundleDescriptor.fromFile(
      new File(bundle, "descriptor.properties"));

    try {
      for (String itemName : bundle.list()) {
        bundleItemPaths.add(new Path(bundle.getAbsolutePath(), itemName));
      }

      fileSystem.copyFromLocalFile(false, true,
        bundleItemPaths.toArray(new Path[bundleItemPaths.size()]), applicationDirectory);
    } catch (IOException e) {
      throw new ApplicationRepositoryException(
        "Failed to copy bundle contents from:"
          + bundle
          + " to directory:"
          + applicationDirectory, e);
    }

    return descriptor;
  }

  @Override
  public void undeploy(String name) {
    Preconditions.checkArgument(name != null, "Name can not be null");

    Path applicationDirectory = new Path(directory, name);

    logger.info("Undeploying application name:{} from:{}", name, applicationDirectory);

    try {
      if (!fileSystem.delete(applicationDirectory, true)) {
        throw new ApplicationRepositoryException(
          "Application directory:" + applicationDirectory + " does not exist");
      }
    } catch (IOException e) {
      throw new ApplicationRepositoryException(
        "Internal error deleting application directory:" + applicationDirectory, e);
    }
  }

  @Override
  public BundleDescriptor getDescriptor(String name) {
    Preconditions.checkArgument(name != null, "Name may not be null");

    Path applicationDirectory = new Path(directory, name);
    Closer closer = Closer.create();
    FSDataInputStream inputStream;

    try {
      return BundleDescriptor.fromInputStream(
        closer.register(
          fileSystem.open(
            new Path(applicationDirectory, "descriptor.properties")
          )
        )
      );
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      try {
        closer.close();
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public InputSupplier<InputStream> getBundleSupplier(String name) {
    Preconditions.checkArgument(name != null, "Name may not be null");

    final Path bundleFile = new Path(new Path(directory, name), name + ".jar");

    return new InputSupplier<InputStream>() {

      @Override
      public InputStream getInput() {
        try {
          return fileSystem.open(bundleFile);
        } catch (IOException e) {
          // TODO: Specialize this exception. -esammer
          throw Throwables.propagate(e);
        }
      }

    };
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileSystem", fileSystem)
      .add("directory", directory)
      .toString();
  }

  public static class Builder implements Supplier<FileSystemApplicationRepository> {

    private FileSystem fileSystem;
    private Path directory;

    public Builder fileSystem(FileSystem fileSystem) {
      this.fileSystem = fileSystem;
      return this;
    }

    public Builder directory(Path directory) {
      this.directory = directory;
      return this;
    }

    public FileSystemApplicationRepository get() {
      Preconditions.checkState(fileSystem != null, "File system may not be null");
      Preconditions.checkState(directory != null, "Directory may not be null");

      return new FileSystemApplicationRepository(fileSystem, directory);
    }
  }
}
