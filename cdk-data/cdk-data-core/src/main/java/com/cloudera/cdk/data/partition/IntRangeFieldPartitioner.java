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
package com.cloudera.cdk.data.partition;

import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;

import java.util.Arrays;

@Beta
public class IntRangeFieldPartitioner extends FieldPartitioner {

  private final int[] upperBounds;

  public IntRangeFieldPartitioner(String name, int... upperBounds) {
    super(name, upperBounds.length);
    this.upperBounds = upperBounds;
  }

  @Override
  public Object apply(Object value) {
    Integer val = (Integer) value;

    for (int i = 0; i < upperBounds.length; i++) {
      if (val <= upperBounds[i]) {
        return i;
      }
    }

    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  public Object valueFromString(String stringValue) {
    return Integer.parseInt(stringValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    IntRangeFieldPartitioner that = (IntRangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(Arrays.asList(this.upperBounds), Arrays.asList(that.upperBounds));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), Arrays.asList(upperBounds));
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
      .add("upperBounds", Arrays.asList(upperBounds)).toString();
  }
}
