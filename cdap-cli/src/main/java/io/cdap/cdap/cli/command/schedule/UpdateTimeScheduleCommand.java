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
package io.cdap.cdap.cli.command.schedule;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.workflow.ScheduleProgramInfo;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.exception.CommandInputError;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.ScheduleClient;
import io.cdap.cdap.internal.schedule.constraint.Constraint;
import io.cdap.cdap.proto.ProtoConstraint;
import io.cdap.cdap.proto.ProtoTrigger;
import io.cdap.cdap.proto.ScheduleDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Updates a schedule.
 */
public final class UpdateTimeScheduleCommand extends AbstractCommand {

  private final ScheduleClient scheduleClient;

  @Inject
  public UpdateTimeScheduleCommand(CLIConfig cliConfig, ScheduleClient scheduleClient) {
    super(cliConfig);
    this.scheduleClient = scheduleClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream printStream) throws Exception {
    String scheduleName = arguments.get(ArgumentName.SCHEDULE_NAME.toString());
    String[] programIdParts = arguments.get(ArgumentName.PROGRAM.toString()).split("\\.");
    String version = arguments.getOptional(ArgumentName.APP_VERSION.toString());
    String scheduleDescription = arguments.getOptional(ArgumentName.DESCRIPTION.toString(), "");
    String cronExpression = arguments.get(ArgumentName.CRON_EXPRESSION.toString());
    String schedulePropertiesString = arguments.getOptional(
        ArgumentName.SCHEDULE_PROPERTIES.toString(), "");
    String scheduleRunConcurrencyString = arguments.getOptional(ArgumentName.CONCURRENCY.toString(),
        null);

    if (programIdParts.length < 2) {
      throw new CommandInputError(this);
    }

    String appId = programIdParts[0];
    NamespaceId namespaceId = cliConfig.getCurrentNamespace();
    ApplicationId applicationId =
        (version == null) ? namespaceId.app(appId) : namespaceId.app(appId, version);
    ScheduleId scheduleId = applicationId.schedule(scheduleName);
    String description = scheduleDescription == null ? null : scheduleDescription;
    ScheduleProgramInfo programInfo = new ScheduleProgramInfo(SchedulableProgramType.WORKFLOW,
        programIdParts[1]);
    List<Constraint> constraints = scheduleRunConcurrencyString == null ? ImmutableList.of() :
        ImmutableList.of(new ProtoConstraint.ConcurrencyConstraint(
            Integer.valueOf(scheduleRunConcurrencyString)));
    Map<String, String> propertiesMap = ArgumentParser.parseMap(schedulePropertiesString,
        ArgumentName.SCHEDULE_PROPERTIES.toString());
    ScheduleDetail scheduleDetail = new ScheduleDetail(scheduleName, description, programInfo,
        propertiesMap,
        new ProtoTrigger.TimeTrigger(cronExpression), constraints, null);
    scheduleClient.update(scheduleId, scheduleDetail);
    printStream.printf("Successfully updated schedule '%s' in app '%s'\n", scheduleName, appId);
  }

  @Override
  public String getPattern() {
    return String.format("update time schedule <%s> for workflow <%s> [version <%s>] "
            + "[description <%s>] at <%s> [concurrency <%s>] [properties <%s>]",
        ArgumentName.SCHEDULE_NAME, ArgumentName.PROGRAM, ArgumentName.APP_VERSION,
        ArgumentName.DESCRIPTION, ArgumentName.CRON_EXPRESSION, ArgumentName.CONCURRENCY,
        ArgumentName.SCHEDULE_PROPERTIES);
  }

  @Override
  public String getDescription() {
    return String.format("Updates %s which is associated with %s given",
        Fragment.of(Article.A, ElementType.SCHEDULE.getName()),
        Fragment.of(Article.THE, ElementType.PROGRAM.getName()));
  }
}
