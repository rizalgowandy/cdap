/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the complete metadata of a {@link MetadataEntity} including its properties, tags in a
 * given {@link MetadataScope} this class was in cdap-api earlier and has been moved from
 * cdap-common as its used only internally
 */
public class MetadataRecord {

  private final MetadataEntity metadataEntity;
  private final MetadataScope scope;
  private final Map<String, String> properties;
  private final Set<String> tags;

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope}.
   */
  public MetadataRecord(NamespacedEntityId entityId, MetadataScope scope) {
    this(entityId.toMetadataEntity(), scope);
  }

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope}.
   */
  public MetadataRecord(MetadataEntity metadataEntity, MetadataScope scope) {
    this(metadataEntity, scope, Collections.emptyMap(), Collections.emptySet());
  }

  /**
   * Returns a new {@link MetadataRecord} from the specified existing {@link MetadataRecord}.
   */
  public MetadataRecord(MetadataRecord other) {
    this(other.getMetadataEntity(), other.getScope(), other.getProperties(), other.getTags());
  }

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope}.
   */
  public MetadataRecord(NamespacedEntityId entityId, MetadataScope scope,
      Map<String, String> properties,
      Set<String> tags) {
    this(entityId.toMetadataEntity(), scope, properties, tags);
  }

  /**
   * Returns an empty {@link MetadataRecord} in the specified {@link MetadataScope} containing the
   * specified properties and tags
   */
  public MetadataRecord(MetadataEntity metadataEntity, MetadataScope scope,
      Map<String, String> properties,
      Set<String> tags) {
    this.metadataEntity = metadataEntity;
    this.scope = scope;
    this.properties = new HashMap<>(properties);
    this.tags = new HashSet<>(tags);
  }

  /**
   * @return the {@link MetadataEntity} whose metadata this {@link MetadataRecord} represents
   */
  public MetadataEntity getMetadataEntity() {
    return metadataEntity;
  }

  /**
   * @return the {@link MetadataScope} of this {@link MetadataRecord}
   */
  public MetadataScope getScope() {
    return scope;
  }

  /**
   * @return the properties of this {@link MetadataRecord}
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  /**
   * @return the tags of this {@link MetadataRecord}
   */
  public Set<String> getTags() {
    return tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetadataRecord that = (MetadataRecord) o;

    return Objects.equals(metadataEntity, that.metadataEntity)
        && scope == that.scope
        && Objects.equals(properties, that.properties)
        && Objects.equals(tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataEntity, scope, properties, tags);
  }

  @Override
  public String toString() {
    return "MetadataRecord{"
        + "metadataEntity=" + metadataEntity
        + ", scope=" + scope
        + ", properties=" + properties
        + ", tags=" + tags
        + '}';
  }
}
