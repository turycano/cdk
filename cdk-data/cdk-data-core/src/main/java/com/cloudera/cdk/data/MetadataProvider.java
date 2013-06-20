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
package com.cloudera.cdk.data;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * <p>
 * Defines a service provider interface for metadata system plugins.
 * </p>
 * <p>
 * Implementations of {@link DatasetRepository} are written against this
 * interface and, by extension, can store dataset metadata information contained
 * in the {@link DatasetDescriptor} in a system of the user's choosing.
 * </p>
 * <p>
 * Implementations of {@link MetadataProvider} are typically not thread-safe; that is,
 * the behavior when accessing a single instance from multiple threads is undefined.
 * </p>
 */
@NotThreadSafe
public interface MetadataProvider {

  /**
   * Load the dataset descriptor for the dataset {@code name}.
   *
   * @param name The fully qualified name of an existing dataset.
   * @return A dataset descriptor.
   * @throws MetadataProviderException If the dataset doesn't exist or the descriptor can not be loaded.
   */
  DatasetDescriptor load(String name);

  /**
   * Save the dataset descriptor for the dataset {@code name}.
   *
   * @param name       The fully qualified name of a dataset.
   * @param descriptor A dataset descriptor.
   * @throws MetadataProviderException If the dataset descriptor can not be saved.
   */
  void save(String name, DatasetDescriptor descriptor);

  /**
   * Delete all metadata associated with the dataset named {@code name}.
   *
   * @param name The fully qualified name of a dataset.
   * @return True if the dataset is successfully deleted, false if the dataset
   *         doesn't exist.
   * @throws MetadataProviderException If the dataset metadata exists but can not be deleted.
   */
  boolean delete(String name);

}
