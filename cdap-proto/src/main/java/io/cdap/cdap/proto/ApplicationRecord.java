/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.proto;

import com.google.gson.annotations.SerializedName;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents item in the list from /apps
 */
public class ApplicationRecord {

  private final String type;
  private final String name;
  private final String version;
  private final String description;
  private final ArtifactSummary artifact;
  @SerializedName("principal")
  private final String ownerPrincipal;
  @Nullable
  private final ChangeDetail change;
  @Nullable
  private final SourceControlMeta sourceControlMeta;

  public ApplicationRecord(ApplicationDetail detail) {
    this(detail.getArtifact(), detail.getName(),
        detail.getAppVersion(), detail.getDescription(),
        detail.getOwnerPrincipal(), detail.getChange(),
        detail.getSourceControlMeta());
  }

  /**
   * Constructor for backwards compatibility, please do not remove.
   */
  public ApplicationRecord(ArtifactSummary artifact, String name, String version,
      String description,
      @Nullable String ownerPrincipal) {
    this.type = "App";
    this.artifact = artifact;
    this.name = name;
    this.description = description;
    this.version = version;
    this.ownerPrincipal = ownerPrincipal;
    this.change = null;
    this.sourceControlMeta = null;
  }

  /**
   * Constructor for backwards compatibility, please do not remove.
   */
  public ApplicationRecord(ArtifactSummary artifact, String name, String version,
      String description,
      @Nullable String ownerPrincipal, @Nullable ChangeDetail change) {
    this.type = "App";
    this.artifact = artifact;
    this.name = name;
    this.description = description;
    this.version = version;
    this.ownerPrincipal = ownerPrincipal;
    this.change = change;
    this.sourceControlMeta = null;
  }

  public ApplicationRecord(ArtifactSummary artifact, String name, String version,
      String description,
      @Nullable String ownerPrincipal, @Nullable ChangeDetail change,
      @Nullable SourceControlMeta sourceControlMeta) {
    this.type = "App";
    this.artifact = artifact;
    this.name = name;
    this.description = description;
    this.version = version;
    this.ownerPrincipal = ownerPrincipal;
    this.change = change;
    this.sourceControlMeta = sourceControlMeta;
  }

  public ArtifactSummary getArtifact() {
    return artifact;
  }

  public String getAppVersion() {
    return version;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  @Nullable
  public String getOwnerPrincipal() {
    return ownerPrincipal;
  }

  @Nullable
  public ChangeDetail getChange() {
    return change;
  }

  @Nullable
  public SourceControlMeta getSourceControlMeta() {
    return sourceControlMeta;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ApplicationRecord that = (ApplicationRecord) o;

    return Objects.equals(type, that.type)
        && Objects.equals(name, that.name)
        && Objects.equals(version, that.version)
        && Objects.equals(description, that.description)
        && Objects.equals(artifact, that.artifact)
        && Objects.equals(ownerPrincipal, that.ownerPrincipal)
        && Objects.equals(change, that.change)
        && Objects.equals(sourceControlMeta, that.sourceControlMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, version, description, artifact, ownerPrincipal, change,
        sourceControlMeta);
  }

  @Override
  public String toString() {
    return "ApplicationRecord{"
        + "type='" + type + '\''
        + ", name='" + name + '\''
        + ", version='" + version + '\''
        + ", description='" + description + '\''
        + ", artifact=" + artifact
        + ", ownerPrincipal='" + ownerPrincipal + '\''
        + ", change=" + change
        + ", sourceControlMeta=" + sourceControlMeta
        + '}';
  }
}
