/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.condition;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.workflow.NodeValue;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.condition.ConditionContext;
import io.cdap.cdap.etl.api.condition.StageStatistics;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.common.AbstractStageContext;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of the {@link ConditionContext}.
 */
public class BasicConditionContext extends AbstractStageContext implements ConditionContext {

  private static final Logger LOG = LoggerFactory.getLogger(BasicConditionContext.class);
  private final WorkflowContext context;
  private final Map<String, StageStatistics> stageStatistics;

  public BasicConditionContext(WorkflowContext context, PipelineRuntime pipelineRuntime,
      StageSpec stageSpec) {
    super(pipelineRuntime, stageSpec);
    this.context = context;
    this.stageStatistics = ImmutableMap.copyOf(createStageStatistics(context));
  }


  private Map<String, StageStatistics> createStageStatistics(WorkflowContext context) {
    Map<String, StageStatistics> stageStatistics = new HashMap<>();
    WorkflowToken token = context.getToken();
    for (WorkflowToken.Scope scope : Arrays.asList(WorkflowToken.Scope.SYSTEM,
        WorkflowToken.Scope.USER)) {
      Map<String, List<NodeValue>> all = token.getAll(scope);
      for (Map.Entry<String, List<NodeValue>> entry : all.entrySet()) {
        if (!entry.getKey().startsWith(Constants.StageStatistics.PREFIX + ".")) {
          continue;
        }
        String stageKey = entry.getKey().substring(Constants.StageStatistics.PREFIX.length() + 1);

        String stageName;
        if (stageKey.endsWith(Constants.StageStatistics.INPUT_RECORDS)) {
          stageName = stageKey.substring(0,
              stageKey.length() - Constants.StageStatistics.INPUT_RECORDS.length() - 1);
        } else if (stageKey.endsWith(Constants.StageStatistics.OUTPUT_RECORDS)) {
          stageName = stageKey.substring(0,
              stageKey.length() - Constants.StageStatistics.OUTPUT_RECORDS.length() - 1);
        } else if (stageKey.endsWith(Constants.StageStatistics.ERROR_RECORDS)) {
          stageName = stageKey.substring(0,
              stageKey.length() - Constants.StageStatistics.ERROR_RECORDS.length() - 1);
        } else {
          // should not happen
          LOG.warn(String.format(
              "Ignoring key '%s' in the Workflow token while generating stage statistics "
                  + "because it is not in the form "
                  + "'stage.statistics.<stage_name>.<input|output|error>.records'.",
              stageKey));
          continue;
        }

        // Since stage names are unique and properties for each stage tracked are unique(input, output, and error)
        // there should only be single node who added this particular key in the Workflow
        long value = entry.getValue().get(0).getValue().getAsLong();

        StageStatistics statistics = stageStatistics.get(stageName);
        if (statistics == null) {
          statistics = new BasicStageStatistics(0, 0, 0);
          stageStatistics.put(stageName, statistics);
        }

        long numOfInputRecords = statistics.getInputRecordsCount();
        long numOfOutputRecords = statistics.getOutputRecordsCount();
        long numOfErrorRecords = statistics.getErrorRecordsCount();

        if (stageKey.endsWith(Constants.StageStatistics.INPUT_RECORDS)) {
          numOfInputRecords = value;
        } else if (stageKey.endsWith(Constants.StageStatistics.OUTPUT_RECORDS)) {
          numOfOutputRecords = value;
        } else {
          numOfErrorRecords = value;
        }
        stageStatistics.put(stageName,
            new BasicStageStatistics(numOfInputRecords, numOfOutputRecords,
                numOfErrorRecords));
      }
    }
    return stageStatistics;
  }

  @Override
  public List<SecureStoreMetadata> list(String namespace) throws Exception {
    return context.list(namespace);
  }

  @Override
  public SecureStoreData get(String namespace, String name) throws Exception {
    return context.get(namespace, name);
  }

  @Override
  public SecureStoreMetadata getMetadata(String namespace, String name) throws Exception {
    return context.getMetadata(namespace, name);
  }

  @Override
  public byte[] getData(String namespace, String name) throws Exception {
    return context.getData(namespace, name);
  }

  @Override
  public void put(String namespace, String name, String data, @Nullable String description,
      Map<String, String> properties) throws Exception {
    context.getAdmin().put(namespace, name, data, description, properties);
  }

  @Override
  public void delete(String namespace, String name) throws Exception {
    context.getAdmin().delete(namespace, name);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    context.execute(runnable);
  }

  @Override
  public void execute(int timeoutInSeconds, TxRunnable runnable)
      throws TransactionFailureException {
    context.execute(timeoutInSeconds, runnable);
  }

  @Override
  public Map<String, StageStatistics> getStageStatistics() {
    return stageStatistics;
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    throw new UnsupportedOperationException("Lineage recording is not supported.");
  }
}
