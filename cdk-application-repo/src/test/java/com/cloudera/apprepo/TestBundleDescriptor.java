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

import com.google.common.io.Resources;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class TestBundleDescriptor {

  private static final Logger logger = LoggerFactory.getLogger(TestBundleDescriptor.class);

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyProps() {
    BundleDescriptor descriptor = new BundleDescriptor(new Properties());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingType() {
    Properties props = new Properties();
    props.setProperty("bundle.version", "1");

    BundleDescriptor descriptor = new BundleDescriptor(props);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingVersion() {
    Properties props = new Properties();
    props.setProperty("bundle.type", "1");

    BundleDescriptor descriptor = new BundleDescriptor(props);
  }

  @Test
  public void testWithFile() {
    BundleDescriptor descriptor = BundleDescriptor.fromFile(
      new File(Resources.getResource("bundles/b1/descriptor.properties").getFile()));

    Assert.assertNotNull("Descriptor is null", descriptor);
    Assert.assertEquals("classpath-dependency", descriptor.getType());
    Assert.assertEquals("1", descriptor.getVersion());

    logger.debug("loaded descriptor:{}", descriptor);
  }

}
