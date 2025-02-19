/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import com.google.common.base.Objects;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.internal.app.ApplicationSpecificationAdapter;
import io.cdap.cdap.proto.artifact.ChangeDetail;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import javax.annotation.Nullable;

/**
 * Holds application metadata
 */
public class ApplicationMeta {

  private static final ApplicationSpecificationAdapter ADAPTER = ApplicationSpecificationAdapter.create();

  private final String id;
  private final ApplicationSpecification spec;
  @Nullable
  private final ChangeDetail change;
  @Nullable
  private final SourceControlMeta sourceControlMeta;

  public ApplicationMeta(String id, ApplicationSpecification spec,
      @Nullable ChangeDetail change, @Nullable SourceControlMeta sourceControlMeta) {
    this.id = id;
    this.spec = spec;
    this.change = change;
    this.sourceControlMeta = sourceControlMeta;
  }

  public ApplicationMeta(String id, ApplicationSpecification spec, @Nullable ChangeDetail change) {
    this(id, spec, change, null);
  }

  public String getId() {
    return id;
  }

  public ApplicationSpecification getSpec() {
    return spec;
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
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", id)
        .add("spec", ADAPTER.toJson(spec))
        .add("change", change)
        .add("sourceControlMeta", sourceControlMeta)
        .toString();
  }
}
