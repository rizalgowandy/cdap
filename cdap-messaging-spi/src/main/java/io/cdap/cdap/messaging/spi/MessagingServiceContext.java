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

package io.cdap.cdap.messaging.spi;

import java.util.List;
import java.util.Map;

/**
 * The context object available to {@link MessagingService} for access to CDAP resources.
 */
public interface MessagingServiceContext {

  /**
   * System properties are derived from the CDAP configuration. Anything in the CDAP configuration
   * will be added as an entry in the system properties.
   *
   * @return unmodifiable system properties for the messaging service.
   */
  Map<String, String> getProperties();

  /**
   * Returns a list of system topics {@link TopicMetadata} as configured by the
   * {@link Constants.MessagingSystem#SYSTEM_TOPICS} property.
   *
   * @return list of system topics {@link TopicMetadata}
   */
  List<TopicMetadata> getSystemTopics();

}