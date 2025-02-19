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

package io.cdap.cdap.logging.logbuffer;

import io.cdap.cdap.logging.meta.Checkpoint;
import io.cdap.cdap.logging.meta.CheckpointManager;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Checkpoint manager for unit tests.
 */
public class MockCheckpointManager implements CheckpointManager<LogBufferFileOffset> {
  private final Map<Integer, Checkpoint<LogBufferFileOffset>> map = new HashMap<>();

  @Override
  public void saveCheckpoints(Map<Integer, ? extends Checkpoint<LogBufferFileOffset>> checkpoints) throws IOException {
    for (Map.Entry<Integer, ? extends Checkpoint<LogBufferFileOffset>> entry : checkpoints.entrySet()) {
      map.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public Map<Integer, Checkpoint<LogBufferFileOffset>> getCheckpoint(Set<Integer> partitions) throws IOException {
    return Collections.emptyMap();
  }

  @Override
  public Checkpoint<LogBufferFileOffset> getCheckpoint(int partition) throws IOException {
    if (!map.containsKey(partition)) {
      return new Checkpoint<>(new LogBufferFileOffset(-1, -1), -1);
    }

    return map.get(partition);
  }
}
