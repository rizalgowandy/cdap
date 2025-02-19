/*
 * Copyright © 2023 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.spi.events;


/**
 * {@link EventReader} Interface for listening for events.
 */
public interface EventReader<T extends Event> extends AutoCloseable {

  /**
   * Returns the identifier for this reader.
   *
   * @return String id for the reader
   */
  String getId();

  /**
   * Method to initialize EventReader.
   *
   * @param context Configuration properties of reader
   */
  void initialize(EventReaderContext context);

  /**
   * Pull messages if available. If no messages are available,
   * the consumer of the returned (empty) {@link EventResult} will not consume any messages.
   *
   * @param maxMessages maximum messages to pull
   * @return Result containing events
   */
  EventResult<T> pull(int maxMessages);
}

