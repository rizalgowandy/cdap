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

package io.cdap.cdap.cli.command.metrics;

import com.google.inject.Inject;
import io.cdap.cdap.cli.CLIConfig;
import io.cdap.cdap.cli.util.AbstractAuthCommand;
import io.cdap.cdap.cli.util.ArgumentParser;
import io.cdap.cdap.client.MetricsClient;
import io.cdap.cdap.proto.MetricTagValue;
import io.cdap.common.cli.Arguments;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Searches metric tags.
 */
public class SearchMetricTagsCommand extends AbstractAuthCommand {

  private final MetricsClient client;

  @Inject
  public SearchMetricTagsCommand(MetricsClient client, CLIConfig cliConfig) {
    super(cliConfig);
    this.client = client;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    Map<String, String> tags = ArgumentParser.parseMap(arguments.getOptional("tags", ""), "<tags>");
    List<MetricTagValue> results = client.searchTags(tags);
    for (MetricTagValue result : results) {
      output.printf("%s=%s\n", result.getName(), result.getValue());
    }
  }

  @Override
  public String getPattern() {
    return "search metric tags [<tags>]";
  }

  @Override
  public String getDescription() {
    return "Searches metric tags. Provide '<tags>' as a map in the format 'tag1=value1 tag2=value2'.";
  }
}
