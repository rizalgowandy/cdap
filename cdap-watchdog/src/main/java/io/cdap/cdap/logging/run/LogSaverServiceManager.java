/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.logging.run;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.twill.AbstractMasterServiceManager;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.discovery.DiscoveryServiceClient;

/**
 * CDAP Log Saver Service Management in Distributed Mode.
 */
public class LogSaverServiceManager extends AbstractMasterServiceManager {

  @Inject
  LogSaverServiceManager(CConfiguration cConf, TwillRunner twillRunner,
      DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.LOGSAVER, twillRunner);
  }

  @Override
  public int getMaxInstances() {
    return getCconf().getInt(Constants.LogSaver.MAX_INSTANCES);
  }

  @Override
  public boolean isLogAvailable() {
    return false;
  }

  @Override
  public String getDescription() {
    return Constants.LogSaver.SERVICE_DESCRIPTION;
  }
}
