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
 * A stream-oriented dataset reader.
 * </p>
 * <p>
 * Subsystem-specific implementations of this interface are used to read data
 * from a {@link Dataset}. Readers are use-once objects that produce entities of
 * type {@code E}. Normally, users are not expected to instantiate
 * implementations directly. Instead, use the containing dataset's
 * {@link Dataset#getReader()} method to get an appropriate implementation.
 * Normally, users receive an instance of this interface from a dataset, call
 * {@link #open()} to prepare for IO operations, invoke {@link #hasNext()} and
 * {@link #read()} as necessary, and {@link #close()} when they are done or no
 * more data exists.
 * </p>
 * <p>
 * Implementations may hold system resources until the {@link #close()} method
 * is called, so users <strong>must</strong> follow the normal try / finally
 * pattern to ensure these resources are properly freed when the reader is
 * exhausted or no longer useful. Do not rely on implementations automatically
 * invoking the {@code close()} method upon object finalization (although
 * implementations are free to do so, if they choose). All implementations must
 * silently ignore multiple invocations of {@code close()} as well as a close of
 * an unopened reader.
 * </p>
 * <p>
 * If any method throws an exception, the reader is no longer valid, and the
 * only method that may be subsequently called is {@code close()}.
 * </p>
 * <p>
 * Implementations of {@link DatasetReader} are typically not thread-safe; that is,
 * the behavior when accessing a single instance from multiple threads is undefined.
 * </p>
 *
 * @param <E> The type of entity produced by this reader.
 */
@NotThreadSafe
public interface DatasetReader<E> {

  /**
   * <p>
   * Open the reader, allocating any necessary resources required to produce
   * entities.
   * </p>
   * <p>
   * This method <strong>must</strong> be invoked prior to any calls of
   * {@link #hasNext()} or {@link #read()}.
   * </p>
   *
   * @throws DatasetReaderException
   */
  void open();

  /**
   * Tests the reader to see if additional entities can be read.
   *
   * @return true if additional entities exist, false otherwise.
   * @throws DatasetReaderException
   */
  boolean hasNext();

  /**
   * <p>
   * Fetch the next entity from the reader.
   * </p>
   * <p>
   * Calling this method when no additional data exists is illegal; users should
   * use {@link #hasNext()} to test if a call to {@code read()} will succeed.
   * Implementations of this method may block.
   * </p>
   *
   * @return An entity of type {@code E}.
   * @throws DatasetReaderException
   */
  E read();

  /**
   * <p>
   * Close the reader and release any system resources.
   * </p>
   * <p>
   * No further operations of this interface (other than additional calls of
   * this method) may be performed, however implementations may choose to permit
   * other method calls. See implementation documentation for details.
   * </p>
   *
   * @throws DatasetReaderException
   */
  void close();

  boolean isOpen();

}
