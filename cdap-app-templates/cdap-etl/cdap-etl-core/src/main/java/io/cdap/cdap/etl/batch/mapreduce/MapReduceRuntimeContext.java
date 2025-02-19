/*
 * Copyright © 2015 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.mapreduce;

import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.mapreduce.MapReduceTaskContext;
import io.cdap.cdap.etl.api.batch.BatchJoinerRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.common.AbstractTransformContext;
import io.cdap.cdap.etl.common.DatasetContextLookupProvider;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.common.plugin.Caller;
import io.cdap.cdap.etl.common.plugin.NoStageLoggingCaller;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Batch runtime context that delegates most operations to MapReduceTaskContext. It also extends
 * {@link AbstractTransformContext} in order to provide plugin isolation between pipeline plugins.
 * This means sources, transforms, and sinks don't need to worry that plugins they use conflict with
 * plugins other sources, transforms, or sinks use.
 */
public class MapReduceRuntimeContext extends AbstractTransformContext
    implements BatchRuntimeContext, BatchJoinerRuntimeContext {

  private static final Caller CALLER = NoStageLoggingCaller.wrap(Caller.DEFAULT);
  private final MapReduceTaskContext context;

  public MapReduceRuntimeContext(MapReduceTaskContext context, PipelineRuntime pipelineRuntime,
      StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec, new DatasetContextLookupProvider(context));
    this.context = context;
  }

  @Override
  public long getLogicalStartTime() {
    return CALLER.callUnchecked(new Callable<Long>() {
      @Override
      public Long call() {
        return context.getLogicalStartTime();
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name) throws DatasetInstantiationException {
    return CALLER.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name)
      throws DatasetInstantiationException {
    return CALLER.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(namespace, name);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String name,
      final Map<String, String> arguments) throws DatasetInstantiationException {
    return CALLER.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(name, arguments);
      }
    });
  }

  @Override
  public <T extends Dataset> T getDataset(final String namespace, final String name,
      final Map<String, String> arguments) throws DatasetInstantiationException {
    return CALLER.callUnchecked(new Callable<T>() {
      @Override
      public T call() throws DatasetInstantiationException {
        return context.getDataset(namespace, name, arguments);
      }
    });
  }

  @Override
  public void releaseDataset(final Dataset dataset) {
    CALLER.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.releaseDataset(dataset);
        return null;
      }
    });
  }

  @Override
  public void discardDataset(final Dataset dataset) {
    CALLER.callUnchecked(new Callable<Void>() {
      @Override
      public Void call() {
        context.discardDataset(dataset);
        return null;
      }
    });
  }
}
