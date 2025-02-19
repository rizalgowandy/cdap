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

package io.cdap.cdap.proto.artifact;

import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A request to get properties for an artifact.
 */
public class ArtifactPropertiesRequest extends ArtifactSummary {

  private final List<String> properties;

  public ArtifactPropertiesRequest(String name, String version, ArtifactScope scope,
      List<String> properties) {
    super(name, version, scope);
    this.properties = Collections.unmodifiableList(new ArrayList<>(properties));
  }

  public List<String> getProperties() {
    return properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ArtifactPropertiesRequest that = (ArtifactPropertiesRequest) o;

    return Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), properties);
  }
}
