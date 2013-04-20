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
import com.google.common.base.Throwables;
import com.google.common.io.Closer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BundleDescriptor {

  private Properties properties;

  public static BundleDescriptor fromInputStream(InputStream inputStream) throws IOException {
    Properties properties = new Properties();

    properties.load(inputStream);

    return new BundleDescriptor(properties);
  }

  public static BundleDescriptor fromFile(File propertyFile) {
    Properties properties = new Properties();
    Closer closer = Closer.create();

    try {
      BufferedInputStream inputStream = closer.register(
        new BufferedInputStream(new FileInputStream(propertyFile)));

      return fromInputStream(inputStream);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
        "Failed to load property file:" + propertyFile + " (can not find file)", e);
    } catch (IOException e) {
      throw new RuntimeException(
        "Failed to load property file:" + propertyFile + " (error reading file)", e);
    } finally {
      try {
        closer.close();
      } catch (IOException e1) {
        throw Throwables.propagate(e1);
      }
    }
  }

  public BundleDescriptor(Properties properties) {
    Preconditions.checkArgument(properties != null, "Properties can not be null");
    Preconditions.checkArgument(properties.containsKey("bundle.type"), "Type property must exist");
    Preconditions.checkArgument(properties.containsKey("bundle.version"), "Version property must exist");

    this.properties = properties;
  }

  public String getType() {
    return get("bundle.type");
  }

  public String getVersion() {
    return get("bundle.version");
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String name) {
    return (T) properties.get(name);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("properties", properties)
      .toString();
  }

}
