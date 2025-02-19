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

package io.cdap.cdap.cli.command;

import com.google.inject.Inject;
import io.cdap.cdap.cli.ArgumentName;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.ElementType;
import io.cdap.cdap.cli.english.Article;
import io.cdap.cdap.cli.english.Fragment;
import io.cdap.cdap.cli.util.AbstractCommand;
import io.cdap.cdap.client.NamespaceClient;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.common.cli.Arguments;
import io.cdap.common.cli.Command;
import java.io.PrintStream;
import jline.console.ConsoleReader;

/**
 * {@link Command} to delete a namespace.
 */
public class DeleteNamespaceCommand extends AbstractCommand {

  private static final String SUCCESS_MSG = "Namespace '%s' deleted successfully.";
  private final NamespaceClient namespaceClient;
  private final CLIConfig cliConfig;

  @Inject
  public DeleteNamespaceCommand(CLIConfig cliConfig, NamespaceClient namespaceClient) {
    super(cliConfig);
    this.cliConfig = cliConfig;
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream out) throws Exception {
    NamespaceId namespaceId = new NamespaceId(
        arguments.get(ArgumentName.NAMESPACE_NAME.toString()));

    ConsoleReader consoleReader = new ConsoleReader();
    if (NamespaceId.DEFAULT.equals(namespaceId)) {
      out.println("WARNING: Deleting contents of a namespace is an unrecoverable operation");
      String prompt = String.format(
          "Are you sure you want to delete contents of namespace '%s' [y/N]? ",
          namespaceId.getNamespace());
      String userConfirm = consoleReader.readLine(prompt);
      if ("y".equalsIgnoreCase(userConfirm)) {
        namespaceClient.delete(namespaceId);
        out.printf("Contents of namespace '%s' were deleted successfully",
            namespaceId.getNamespace());
        out.println();
      }
    } else {
      out.println("WARNING: Deleting a namespace is an unrecoverable operation");
      String prompt = String.format("Are you sure you want to delete namespace '%s' [y/N]? ",
          namespaceId.getNamespace());
      String userConfirm = consoleReader.readLine(prompt);
      if ("y".equalsIgnoreCase(userConfirm)) {
        namespaceClient.delete(namespaceId);
        out.println(String.format(SUCCESS_MSG, namespaceId.getNamespace()));
        if (cliConfig.getCurrentNamespace().equals(namespaceId)) {
          cliConfig.setNamespace(NamespaceId.DEFAULT);
          out.printf("Now using namespace '%s'", NamespaceId.DEFAULT.getNamespace());
          out.println();
        }
      }
    }
  }

  @Override
  public String getPattern() {
    return String.format("delete namespace <%s>", ArgumentName.NAMESPACE_NAME);
  }

  @Override
  public String getDescription() {
    return String.format("Deletes %s", Fragment.of(Article.A, ElementType.NAMESPACE.getName()));
  }
}
