/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.entity.EntityExistenceVerifier;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.proto.id.ArtifactId;
import java.io.IOException;

/**
 * {@link EntityExistenceVerifier} for {@link ArtifactId artifacts}.
 */
public class ArtifactExistenceVerifier implements EntityExistenceVerifier<ArtifactId> {

  private final ArtifactStore artifactStore;

  @Inject
  ArtifactExistenceVerifier(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  @Override
  public void ensureExists(ArtifactId artifactId) throws ArtifactNotFoundException {
    try {
      artifactStore.getArtifact(Id.Artifact.fromEntityId(artifactId));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
