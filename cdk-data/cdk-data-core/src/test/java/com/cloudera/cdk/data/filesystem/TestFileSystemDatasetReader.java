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

import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetReader;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.cloudera.cdk.data.filesystem.DatasetTestUtilities.STRING_SCHEMA;

public class TestFileSystemDatasetReader {

  private FileSystem fileSystem;

  @Before
  public void setUp() throws IOException {
    fileSystem = FileSystem.get(new Configuration());
  }

  @Test
  public void testRead() throws IOException {
    DatasetReader<Record> reader;
    int records = 0;

    reader = new FileSystemDatasetReader<Record>(fileSystem, new Path(Resources
        .getResource("data/strings-100.avro").getFile()), STRING_SCHEMA);

    try {
      reader.open();

      while (reader.hasNext()) {
        Record record = reader.read();

        Assert.assertNotNull(record);
        Assert.assertEquals(String.valueOf(records), record.get("text"));
        records++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(100, records);
  }

  @Test
  public void testEvolvedSchema() throws IOException {
    Schema schema = Schema.createRecord("mystring", null, null, false);
    schema.setFields(Lists.newArrayList(
        new Field("text", Schema.create(Type.STRING), null, null),
        new Field("text2", Schema.create(Type.STRING), null,
            JsonNodeFactory.instance.textNode("N/A"))));

    FileSystemDatasetReader<Record> reader = new FileSystemDatasetReader<Record>(
        fileSystem, new Path(Resources.getResource("data/strings-100.avro")
            .getFile()), schema);
    int records = 0;

    try {
      reader.open();

      while (reader.hasNext()) {
        Record record = reader.read();

        Assert.assertEquals(String.valueOf(records), record.get("text"));
        Assert.assertEquals("N/A", record.get("text2"));
        records++;
      }
    } finally {
      reader.close();
    }

    Assert.assertEquals(100, records);
  }

  @Test(expected = IllegalStateException.class)
  public void testHasNextOnNonOpenWriterFails() throws IOException {
    DatasetReader<String> reader;
    reader = new FileSystemDatasetReader<String>(fileSystem, new Path(Resources
        .getResource("data/strings-100.avro").getFile()), STRING_SCHEMA);

    try {
      reader.hasNext();
    } finally {
      reader.close();
    }
  }

}
