/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.exec;

import io.cdap.cdap.etl.api.Destroyable;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transformation;
import io.cdap.cdap.etl.common.Destroyables;
import io.cdap.cdap.etl.common.RecordInfo;


/**
 * Processing any stages that can be represented as a Transformation. Gets the value from a
 * RecordInfo before passing it on to the underlying transformation.
 *
 * @param <T> type of input object
 */
public class UnwrapPipeStage<T> extends PipeStage<RecordInfo<T>> {

  private final Transformation<T, Object> transform;
  private final Emitter<Object> emitter;

  public UnwrapPipeStage(String stageName, Transformation<T, Object> transform,
      Emitter<Object> emitter) {
    super(stageName);
    this.transform = transform;
    this.emitter = emitter;
  }

  @Override
  public void consumeInput(RecordInfo<T> input) throws Exception {
    transform.transform(input.getValue(), emitter);
  }

  @Override
  public void destroy() {
    if (transform instanceof Destroyable) {
      Destroyables.destroyQuietly((Destroyable) transform);
    }
  }

}
