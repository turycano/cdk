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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import java.util.Map;

public class ApplicationExecutorFactory {

  private ApplicationRepository repository;
  private Map<String, ApplicationExecutorDelegate> executors;

  public ApplicationExecutorFactory(ApplicationRepository repository,
    Map<String, ApplicationExecutorDelegate> executors) {

    Preconditions.checkArgument(repository != null, "Repository may not be null");
    Preconditions.checkArgument(executors != null, "Executors may not be null");

    this.repository = repository;
    this.executors = executors;
  }

  public ApplicationExecutor getExecutor(String name) {
    BundleDescriptor descriptor = repository.getDescriptor(name);

    Preconditions.checkState(executors.containsKey(descriptor.getType()),
      "No configured executor for descriptor type:%s", descriptor.getType());

    ApplicationExecutorDelegate delegate = executors.get(descriptor.getType());

    return new ApplicationExecutor(delegate, descriptor);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("repository", repository)
      .add("executors", executors)
      .toString();
  }

  public static class Builder implements Supplier<ApplicationExecutorFactory> {

    private ApplicationRepository repository;
    private Map<String, ApplicationExecutorDelegate> executors;

    public Builder() {
      executors = Maps.newHashMap();
    }

    public Builder repository(ApplicationRepository repository) {
      this.repository = repository;
      return this;
    }

    public Builder registerExecutor(String typeName, ApplicationExecutorDelegate executor) {
      executors.put(typeName, executor);
      return this;
    }

    public Builder executors(Map<String, ApplicationExecutorDelegate> executors) {
      this.executors = executors;
      return this;
    }

    @Override
    public ApplicationExecutorFactory get() {
      Preconditions.checkArgument(repository != null, "Repository may not be null");
      Preconditions.checkArgument(executors != null, "Executors may not be null");

      return new ApplicationExecutorFactory(repository, executors);
    }

  }

}
