/**
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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.filesystem.impl.Accessor;
import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.filesystem.impl.Accessor;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;

final class AccessorImpl extends Accessor {

  @Override
  public Path getDirectory(Dataset dataset) {
    if (dataset instanceof FileSystemDataset) {
      return ((FileSystemDataset) dataset).getDirectory();
    }
    return null;
  }

  @Override
  public void accumulateDatafilePaths(Dataset dataset, Path directory,
      List<Path> paths) throws IOException {
    if (dataset instanceof FileSystemDataset) {
      ((FileSystemDataset) dataset).accumulateDatafilePaths(directory, paths);
    }
  }
}
