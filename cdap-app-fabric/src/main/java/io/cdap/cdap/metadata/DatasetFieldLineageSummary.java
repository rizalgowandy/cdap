/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.id.DatasetId;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The summary about all the field level lineage information about all fields in a dataset.
 */
public class DatasetFieldLineageSummary {

  private final Constants.FieldLineage.Direction direction;
  @SerializedName("start-ts")
  private final long startTs;
  @SerializedName("end-ts")
  private final long endTs;
  @SerializedName("entityId")
  private final DatasetId datasetId;
  private final Set<String> fields;
  private final Set<FieldLineageRelations> incoming;
  private final Set<FieldLineageRelations> outgoing;

  public DatasetFieldLineageSummary(Constants.FieldLineage.Direction direction, long startTs,
      long endTs,
      DatasetId datasetId, Set<String> fields,
      Map<DatasetId, Integer> fieldCounts,
      Map<DatasetId, Set<FieldRelation>> incoming,
      Map<DatasetId, Set<FieldRelation>> outgoing) {
    this.direction = direction;
    this.startTs = startTs;
    this.endTs = endTs;
    this.datasetId = datasetId;
    this.fields = fields;
    this.incoming = incoming.entrySet().stream().map(
        entry -> new FieldLineageRelations(entry.getKey(),
            fieldCounts.getOrDefault(entry.getKey(), 0),
            entry.getValue())).collect(Collectors.toSet());
    this.outgoing = outgoing.entrySet().stream().map(
        entry -> new FieldLineageRelations(entry.getKey(),
            fieldCounts.getOrDefault(entry.getKey(), 0),
            entry.getValue())).collect(Collectors.toSet());
  }

  public Constants.FieldLineage.Direction getDirection() {
    return direction;
  }

  public long getStartTs() {
    return startTs;
  }

  public long getEndTs() {
    return endTs;
  }

  public DatasetId getDatasetId() {
    return datasetId;
  }

  public Set<String> getFields() {
    return fields;
  }

  public Set<FieldLineageRelations> getIncoming() {
    return incoming;
  }

  public Set<FieldLineageRelations> getOutgoing() {
    return outgoing;
  }

  /**
   * This class represents an aggregation of the field level lineage about an incoming/outgoing
   * dataset. The entityId represents the dataset. And the relations contains all the field level
   * lineage.
   */
  public static class FieldLineageRelations {

    @SerializedName("entityId")
    private final DatasetId datasetId;
    private final int fieldCount;
    private final Set<FieldRelation> relations;

    // this datasetId can be nullable to represent the field is not related to any other dataset but still
    // have other operations around itself, i.e, drop or generate
    @VisibleForTesting
    public FieldLineageRelations(@Nullable DatasetId datasetId, int fieldCount,
        Set<FieldRelation> relations) {
      this.datasetId = datasetId;
      this.fieldCount = fieldCount;
      this.relations = relations;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      DatasetFieldLineageSummary.FieldLineageRelations that = (DatasetFieldLineageSummary.FieldLineageRelations) o;
      return Objects.equals(datasetId, that.datasetId)
          && Objects.equals(relations, that.relations)
          && Objects.equals(fieldCount, that.fieldCount);
    }

    @Override
    public int hashCode() {
      return Objects.hash(datasetId, fieldCount, relations);
    }

    public DatasetId getDatasetId() {
      return datasetId;
    }

    public int getFieldCount() {
      return fieldCount;
    }

    public Set<FieldRelation> getRelations() {
      return relations;
    }
  }
}
