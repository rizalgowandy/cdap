/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.etl.api;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * An alert emitted by a stage in the pipeline.
 */
public class Alert implements Serializable {

  private static final long serialVersionUID = -3280276088177879979L;
  private final String stageName;
  private final Map<String, String> payload;

  public Alert(String stageName, Map<String, String> payload) {
    this.stageName = stageName;
    this.payload = Collections.unmodifiableMap(payload);
  }

  /**
   * @return the stage the alert was emitted from
   */
  public String getStageName() {
    return stageName;
  }

  /**
   * @return the unmodifiable alert payload.
   */
  public Map<String, String> getPayload() {
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Alert that = (Alert) o;

    return Objects.equals(stageName, that.stageName) && Objects.equals(payload, that.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stageName, payload);
  }

  @Override
  public String toString() {
    return "Alert{"
        + "stageName='" + stageName + '\''
        + ", payload=" + payload
        + '}';
  }
}
