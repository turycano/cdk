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

import java.util.Arrays;
import java.util.List;

import com.cloudera.cdk.data.FieldPartitioner;
import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

@Beta
public class RangeFieldPartitioner extends FieldPartitioner {

  private final List<Comparable<?>> upperBounds;

  public RangeFieldPartitioner(String name, Comparable<?>... upperBounds) {

    super(name, upperBounds.length);
    this.upperBounds = Arrays.asList(upperBounds);
  }

  @Override
  public Object apply(Object value) {
    Preconditions.checkArgument(value instanceof Comparable<?>,
        "Unable to range partition a value that isn't comparable:%s", value);

    @SuppressWarnings("unchecked")
    Comparable<? super Object> val = (Comparable<? super Object>) value;

    for (Comparable<?> comparable : upperBounds) {
      if (val.compareTo(comparable) <= 0) {
        return comparable;
      }
    }

    throw new IllegalArgumentException(value + " is outside bounds");
  }

  @Override
  public Object valueFromString(String stringValue) {
    return stringValue; // TODO: need more type information
  }

  public List<Comparable<?>> getUpperBounds() {
    return upperBounds;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    RangeFieldPartitioner that = (RangeFieldPartitioner) o;
    return Objects.equal(this.getName(), that.getName()) &&
        Objects.equal(this.upperBounds, that.upperBounds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), upperBounds);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", getName())
        .add("upperBounds", upperBounds).toString();
  }
}
