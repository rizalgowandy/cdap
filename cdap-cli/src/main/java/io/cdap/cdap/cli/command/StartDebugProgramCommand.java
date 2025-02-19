/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli.command;

import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.client.ProgramClient;

/**
 * Starts a program in debug mode.
 */
public class StartDebugProgramCommand extends StartProgramCommand {

  public StartDebugProgramCommand(ElementType elementType, ProgramClient programClient,
      CLIConfig cliConfig) {
    super(elementType, programClient, cliConfig);
    this.isDebug = true;
  }

  @Override
  public String getPattern() {
    return String.format("start-debug %s <%s> [<%s>]", elementType.getShortName(),
        elementType.getArgumentName(),
        ArgumentName.RUNTIME_ARGS);
  }

  @Override
  public String getDescription() {
    return String.format(
        "Starts %s in debug mode. '<%s>' is specified in the format 'key1=a key2=b'.",
        Fragment.of(Article.A, elementType.getName()), ArgumentName.RUNTIME_ARGS);
  }
}
