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

package io.cdap.cdap.cli.commandset;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.cli.command.CallServiceCommand;
import io.cdap.cdap.cli.command.CheckServiceAvailabilityCommand;
import io.cdap.cdap.cli.command.GetServiceEndpointsCommand;
import io.cdap.common.cli.Command;
import io.cdap.common.cli.CommandSet;

/**
 * Service commands.
 */
public class ServiceCommands extends CommandSet<Command> {

  @Inject
  public ServiceCommands(Injector injector) {
    super(
        ImmutableList.<Command>builder()
            .add(injector.getInstance(CallServiceCommand.class))
            .add(injector.getInstance(GetServiceEndpointsCommand.class))
            .add(injector.getInstance(CheckServiceAvailabilityCommand.class))
            .build());
  }
}
